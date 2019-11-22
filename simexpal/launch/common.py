
import os
import selectors
import signal
import subprocess
import time

import yaml

from .. import base
from .. import util

class Launcher:
	pass

def lock_run(run):
	(exp, instance) = (run.experiment, run.instance.filename)
	util.try_mkdir(os.path.join(run.config.basedir, 'aux'))
	util.try_mkdir(os.path.join(run.config.basedir, 'output'))
	util.try_mkdir(exp.aux_subdir)
	util.try_mkdir(exp.output_subdir)

	# We will try to launch the experiment.
	# First, create a .lock file. If that is successful, we are the process that
	# gets to launch the experiment. Afterwards, concurrent access to our files
	# can be considered a bug (or deliberate misuse) and will lead to hard failues.
	try:
		lockfd = os.open(run.aux_file_path('lock'),
				os.O_RDONLY | os.O_CREAT | os.O_EXCL, mode=0)
	except FileExistsError:
		# TODO: Those warnings should be behind a flag.
#				print("Warning: .lock file exists for experiment '{}', instance '{}'".format(
#						exp.name, instance))
#				print("Either experiments are launched concurrently or the launcher crashed.")
		return False
	os.close(lockfd)
	return True

def create_run_file(run):
	(exp, instance) = (run.experiment, run.instance.filename)

	# Create the .run file. This signals that the run has been submitted.
	with open(run.aux_file_path('run.tmp'), "w") as f:
		pass
	os.rename(run.aux_file_path('run.tmp'), run.aux_file_path('run'))

# Stores all information that is necessary to invoke a run.
# This is a view over a POD object which can be YAML-encoded and sent
# over a wire or stored into a file.
class RunManifest:
	def __init__(self, yml):
		self.yml = yml

	@property
	def base_dir(self):
		return self.yml['config']['base_dir']

	@property
	def instance_dir(self):
		return self.yml['config']['instance_dir']

	@property
	def revision(self):
		return self.yml['revision']

	@property
	def instance(self):
		return self.yml['instance']

	@property
	def experiment(self):
		return self.yml['experiment']

	@property
	def repetition(self):
		return self.yml['repetition']

	@property
	def args(self):
		return self.yml['args']

	@property
	def environ(self):
		return self.yml['environ']

	@property
	def output(self):
		return self.yml['output']

	@property
	def timeout(self):
		return self.yml['timeout']

	@property
	def aux_subdir(self):
		return base.get_aux_subdir(self.base_dir, self.experiment,
				[var_yml['name'] for var_yml in self.yml['variants']],
				self.revision)

	@property
	def output_subdir(self):
		return base.get_output_subdir(self.base_dir, self.experiment,
				[var_yml['name'] for var_yml in self.yml['variants']],
				self.revision)

	def aux_file_path(self, ext):
		return os.path.join(self.aux_subdir,
				base.get_aux_file_name(ext, self.instance, self.repetition))

	def output_file_path(self, ext):
		return os.path.join(self.output_subdir,
				base.get_output_file_name(ext, self.instance, self.repetition))

	def get_extra_args(self):
		extra_args = []
		for var_yml in self.yml['variants']:
			extra_args.extend(var_yml['extra_args'])
		return extra_args

	def get_paths(self):
		paths = []
		for build_yml in self.yml['builds']:
			paths.append(os.path.join(build_yml['prefix'], 'bin'))
		return paths

	def get_ldso_paths(self):
		paths = []
		for build_yml in self.yml['builds']:
			paths.append(os.path.join(build_yml['prefix'], 'lib64'))
			paths.append(os.path.join(build_yml['prefix'], 'lib'))
		return paths

	def get_python_paths(self):
		paths = []
		for build_yml in self.yml['builds']:
			for export in build_yml['exports_python']:
				paths.append(os.path.join(build_yml['prefix'], export))
		return paths

def compile_manifest(run):
	exp = run.experiment

	# Perform a DFS to discover all used builds.
	recursive_builds = []
	builds_visited = set()

	for name in exp.info.used_builds:
		assert name not in builds_visited
		build = run.config.get_build(name, exp.revision)
		recursive_builds.append(build)
		builds_visited.add(name)

	i = 0 # Need index-based loop as recursive_builds is mutated in the loop.
	while i < len(recursive_builds):
		build = recursive_builds[i]
		for req_name in build.info.requirements:
			if req_name in builds_visited:
				continue
			req_build = run.config.get_build(req_name, exp.revision)
			recursive_builds.append(req_build)
			builds_visited.add(req_name)
		i += 1

	builds_yml = []
	for build in recursive_builds:
		builds_yml.append({
			'prefix': build.prefix_dir,
			'exports_python': build.info.exports_python
		})

	# Collect extra arguments from variants
	variants_yml = []
	for variant in exp.variation:
		variants_yml.append({
			'name': variant.name,
			'extra_args': variant.variant_yml['extra_args']
		})

	timeout = None
	if 'timeout' in exp.info._exp_yml:
		timeout = float(exp.info._exp_yml['timeout'])

	environ = {}
	if 'environ' in exp.info._exp_yml:
		for (k, v) in exp.info._exp_yml['environ'].items():
			environ[k] = str(v)

	return RunManifest({
		'config': {
			'base_dir': run.config.basedir,
			'instance_dir': run.config.instance_dir()
		},
		'experiment': exp.name,
		'variants': variants_yml,
		'revision': exp.revision.name if exp.revision else None,
		'instance': run.instance.filename,
		'repetition': run.repetition,
		'builds': builds_yml,
		'args': exp.info._exp_yml['args'],
		'timeout': timeout,
		'environ': environ,
		'output': exp.info._exp_yml['output'] if 'output' in exp.info._exp_yml else None
	})

# Dumps data from an FD to the FS.
# Creates the output file only if something is written.
class LazyWriter:
	def __init__(self, fd, path):
		self._fd = fd
		self._path = path
		self._out = None

	def progress(self):
		# Specify some chunk size to avoid reading the whole pipe at once.
		chunk = os.read(self._fd, 16 * 1024)
		if not len(chunk):
			return False

		if self._out is None:
			self._out = open(self._path, "wb")
		self._out.write(chunk)
		self._out.flush()
		return True

class Invocation:
	def __init__(self):
		self.selector = None
		self.outfile = None
		self.stdout = None
		self.stderr = None
		self.stdout_writer = None
		self.stderr_writer = None
		self.stdout_pipe = None
		self.stderr_pipe = None
		self.process = None
		self.wait_for_process = None
		self.wait_for_stdout = None
		self.wait_for_stderr = None

	def __enter__(self):
		self.selector = selectors.DefaultSelector()
		self.wait_for_process = False
		self.wait_for_stdout = False
		self.wait_for_stderr = False
		return self

	def __exit__(self, *_):
		# TODO: kill the process if it is still running?

		if self.outfile is not None:
			os.close(self.outfile)
		if self.stdout is not None:
			os.close(self.stdout)
		if self.stderr is not None:
			os.close(self.stderr)
		if self.stdout_pipe is not None:
			os.close(self.stdout_pipe)
		if self.stderr_pipe is not None:
			os.close(self.stderr_pipe)
		self.selector.close()

		self.selector = None
		self.outfile = None
		self.stdout = None
		self.stderr = None
		self.stdout_writer = None
		self.stderr_writer = None
		self.stdout_pipe = None
		self.stderr_pipe = None
		self.process = None

	def run(self, manifest):
		with open(manifest.output_file_path('out'), "w") as f:
			self.outfile = os.dup(f.fileno())

		# Create the output file. This signals that the run has been started.
		if manifest.output == 'stdout':
			self.stdout = os.dup(self.outfile)
		else:
			(self.stdout_pipe, self.stdout) = os.pipe()
			os.set_blocking(self.stdout_pipe, False)

		# Create the error file.
		(self.stderr_pipe, self.stderr) = os.pipe()
		os.set_blocking(self.stderr_pipe, False)

		def substitute(p):
			if p == 'INSTANCE':
				return manifest.instance_dir + '/' + manifest.instance
			elif p == 'REPETITION':
				return str(manifest.repetition)
			elif p == 'OUTPUT':
				return manifest.output_file_path('out')
			else:
				return None

		def substitute_list(p):
			if p == 'EXTRA_ARGS':
				return manifest.get_extra_args()
			else:
				return None

		cmd = util.expand_at_params(manifest.args, substitute, listfn=substitute_list)

		# Build the environment.
		def prepend_env(var, items):
			if(var in os.environ):
				return ':'.join(items) + ':' + os.environ[var]
			return ':'.join(items)

		environ = os.environ.copy()
		environ['PATH'] = prepend_env('PATH', manifest.get_paths())
		environ['LD_LIBRARY_PATH'] = prepend_env('LD_LIBRARY_PATH', manifest.get_ldso_paths())
		environ['PYTHONPATH'] = prepend_env('PYTHONPATH', manifest.get_python_paths())
		environ.update(manifest.environ)

		if manifest.output != 'stdout':
			self.stdout_writer = LazyWriter(self.stdout_pipe, manifest.aux_file_path('stdout'))
			self.selector.register(self.stdout_pipe, selectors.EVENT_READ)
			self.wait_for_stdout = True
		self.stderr_writer = LazyWriter(self.stderr_pipe, manifest.aux_file_path('stderr'))
		self.selector.register(self.stderr_pipe, selectors.EVENT_READ)
		self.wait_for_stderr = True

		# Launch the process.
		# We also need to close our copies of the FDs so that the selector handles EOF correctly.
		self.process = subprocess.Popen(cmd, cwd=manifest.base_dir, env=environ,
				stdout=self.stdout, stderr=self.stderr)
		os.close(self.stdout)
		self.stdout = None
		os.close(self.stderr)
		self.stderr = None
		self.wait_for_process = True
		start = time.perf_counter()

		# Monitor the process.
		while (self.wait_for_process or self.wait_for_stdout or self.wait_for_stderr):
			# Check whether the process terminated.
			if self.wait_for_process:
				if self.process.poll() is None:
					elapsed = time.perf_counter() - start
					if manifest.timeout is not None and elapsed > manifest.timeout:
						self.process.send_signal(signal.SIGXCPU)
				else:
					self.wait_for_process = False
					continue

			# Check the status of all FDs.
			events = self.selector.select(timeout=1)
			for (sk, mask) in events:
				assert sk.fd is not None
				if sk.fd == self.stdout_pipe:
					assert self.wait_for_stdout
					if not self.stdout_writer.progress():
						self.selector.unregister(self.stdout_pipe)
						self.wait_for_stdout = False
				elif sk.fd == self.stderr_pipe:
					assert self.wait_for_stderr
					if not self.stderr_writer.progress():
						self.selector.unregister(self.stderr_pipe)
						self.wait_for_stderr = False
				else:
					raise RuntimeError('unexpected event from selector')

		runtime = time.perf_counter() - start

		# Collect the status information.
		status = None
		sigcode = None
		if self.process.returncode < 0: # Killed by a signal?
			sigcode = signal.Signals(-self.process.returncode).name
		else:
			status = self.process.returncode
		did_timeout = manifest.timeout is not None and runtime > manifest.timeout

		# Create the status file to signal that we are finished.
		status_dict = {'timeout': did_timeout, 'walltime': runtime,
				'status': status, 'signal': sigcode}
		with open(manifest.output_file_path('status.tmp'), "w") as f:
			yaml.dump(status_dict, f)
		os.rename(manifest.output_file_path('status.tmp'), manifest.output_file_path('status'))

def invoke_run(manifest):
	with Invocation() as invc:
		invc.run(manifest)
