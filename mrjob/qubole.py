from __future__ import with_statement

import logging
import os
import os.path
import pipes
import posixpath
import random
import re
import signal
import socket
import time
import urllib2
from collections import defaultdict
from datetime import datetime
from datetime import timedelta
from subprocess import Popen
from subprocess import PIPE

try:
    from cStringIO import StringIO
    StringIO  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    from StringIO import StringIO

try:
    import simplejson as json  # preferred because of C speedups
    json  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    import json  # built in to Python 2.6 and later

try:
    import boto
    import boto.ec2
    import boto.emr
    import boto.emr.connection
    import boto.emr.instance_group
    import boto.exception
    import boto.regioninfo
    import boto.utils
    boto  # quiet "redefinition of unused ..." warning from pyflakes
except ImportError:
    # don't require boto; MRJobs don't actually need it when running
    # inside hadoop streaming
    boto = None

# need this to retry on SSL errors (see Issue #621)
try:
    from boto.https_connection import InvalidCertificateException
    InvalidCertificateException  # quiet pyflakes warning
except ImportError:
    InvalidCertificateException = None

import mrjob
import mrjob.step
from mrjob.aws import EC2_INSTANCE_TYPE_TO_COMPUTE_UNITS
from mrjob.aws import EC2_INSTANCE_TYPE_TO_MEMORY
from mrjob.aws import MAX_STEPS_PER_JOB_FLOW
from mrjob.aws import s3_endpoint_for_region
from mrjob.aws import s3_location_constraint_for_region
from mrjob.conf import combine_cmds
from mrjob.conf import combine_dicts
from mrjob.conf import combine_lists
from mrjob.conf import combine_path_lists
from mrjob.conf import combine_paths
from mrjob.fs.composite import CompositeFilesystem
from mrjob.fs.local import LocalFilesystem
from mrjob.fs.s3 import S3Filesystem
from mrjob.fs.s3 import wrap_aws_conn
from mrjob.fs.ssh import SSHFilesystem
from mrjob.parse import HADOOP_STREAMING_JAR_RE
from mrjob.parse import is_s3_uri
from mrjob.parse import is_uri
from mrjob.parse import iso8601_to_datetime
from mrjob.parse import iso8601_to_timestamp
from mrjob.parse import parse_s3_uri
from mrjob.pool import est_time_to_hour
from mrjob.pool import pool_hash_and_name
from mrjob.retry import RetryGoRound
from mrjob.runner import MRJobRunner
from mrjob.runner import RunnerOptionStore
from mrjob.setup import BootstrapWorkingDirManager
from mrjob.setup import UploadDirManager
from mrjob.setup import parse_legacy_hash_path
from mrjob.setup import parse_setup_cmd
from mrjob.ssh import SSH_LOG_ROOT
from mrjob.ssh import SSH_PREFIX
from mrjob.ssh import ssh_copy_key
from mrjob.ssh import ssh_slave_addresses
from mrjob.ssh import ssh_terminate_single_job
from mrjob.util import cmd_line
from mrjob.util import hash_object
from mrjob.util import shlex_split

# Qubole Python SDK: https://github.com/qubole/qds-sdk-py
from qds_sdk.commands import *
QUBOLE_HADOOP_VERSION = '0.20.1-dev'
QUBOLE_POLL_TIME = 6
QUBOLE_ANALYZE_URL = 'https://api.qubole.com/v2/analyze?command_id='

log = logging.getLogger(__name__)

# not all steps generate task attempt logs. for now, conservatively check for
# streaming steps, which always generate them.
LOG_GENERATING_STEP_NAME_RE = HADOOP_STREAMING_JAR_RE


MAX_SSH_RETRIES = 20

# ssh should fail right away if it can't bind a port
WAIT_FOR_SSH_TO_FAIL = 1.0

# amount of time to wait between checks for available pooled job flows
JOB_FLOW_SLEEP_INTERVAL = 30.01  # Add .1 seconds so minutes arent spot on.

# Deprecated as of v0.4.1 (will be removed in v0.5).
# Use mrjob.aws.emr_endpoint_for_region() instead
REGION_TO_EMR_ENDPOINT = {
    'us-east-1': 'elasticmapreduce.us-east-1.amazonaws.com',
    'us-west-1': 'elasticmapreduce.us-west-1.amazonaws.com',
    'us-west-2': 'elasticmapreduce.us-west-2.amazonaws.com',
    'EU': 'elasticmapreduce.eu-west-1.amazonaws.com',  # for compatibility
    'eu-west-1': 'elasticmapreduce.eu-west-1.amazonaws.com',
    'ap-southeast-1': 'elasticmapreduce.ap-southeast-1.amazonaws.com',
    'ap-northeast-1': 'elasticmapreduce.ap-northeast-1.amazonaws.com',
    'sa-east-1': 'elasticmapreduce.sa-east-1.amazonaws.com',
    '': 'elasticmapreduce.amazonaws.com',  # when no region specified
}

# Deprecated as of v0.4.1 (will be removed in v0.5).
# Use mrjob.aws.s3_endpoint_for_region() instead
REGION_TO_S3_ENDPOINT = {
    'us-east-1': 's3.amazonaws.com',  # no region-specific endpoint
    'us-west-1': 's3-us-west-1.amazonaws.com',
    'us-west-2': 's3-us-west-2.amazonaws.com',
    'EU': 's3-eu-west-1.amazonaws.com',
    'eu-west-1': 's3-eu-west-1.amazonaws.com',
    'ap-southeast-1': 's3-ap-southeast-1.amazonaws.com',
    'ap-northeast-1': 's3-ap-northeast-1.amazonaws.com',
    'sa-east-1': 's3-sa-east-1.amazonaws.com',
    '': 's3.amazonaws.com',
}

# Deprecated as of v0.4.1 (will be removed in v0.5).
# Use mrjob.aws.s3_location_constraint_for_region() instead
REGION_TO_S3_LOCATION_CONSTRAINT = {
    'us-east-1': '',
}

# bootstrap action which automatically terminates idle job flows
_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH = os.path.join(
    os.path.dirname(mrjob.__file__),
    'bootstrap',
    'terminate_idle_job_flow.sh')


def s3_key_to_uri(s3_key):
    """Convert a boto Key object into an ``s3://`` URI"""
    return 's3://%s/%s' % (s3_key.bucket.name, s3_key.name)


def _lock_acquire_step_2(key, job_name):
    key_value = key.get_contents_as_string()
    return (key_value == job_name)


class LogFetchError(Exception):
    pass

class QuboleError(Exception):
    pass

class QuboleRunnerOptionStore(RunnerOptionStore):

    # documentation of these options is in docs/guides/emr-opts.rst

    ALLOWED_KEYS = RunnerOptionStore.ALLOWED_KEYS.union(set([
        'aws_access_key_id',
        'aws_secret_access_key',
        'api_token',
        'api_version',
        'cluster_label',
        'ami_version',
        'aws_availability_zone',
        'aws_region',
        'qubole_bootstrap',
        'bootstrap_actions',
        'bootstrap_cmds',
        'bootstrap_files',
        'bootstrap_python_packages',
        'bootstrap_scripts',
        'ec2_key_pair',
        'ec2_key_pair_file',
        'hadoop_streaming_jar_on_emr',
        'hadoop_version',
        'iam_job_flow_role',
        'max_hours_idle',
        'mins_to_end_of_hour',
        'num_ec2_core_instances',
        'pool_wait_minutes',
        'num_ec2_instances',
        'num_ec2_task_instances',
        's3_endpoint',
        's3_log_uri',
        's3_scratch_uri',
        's3_sync_wait_time',
        'ssh_bin',
        'ssh_bind_ports',
        'ssh_tunnel_is_open',
        'ssh_tunnel_to_job_tracker',
        'visible_to_all_users',
        'emr_job_flow_id',
    ]))

    COMBINERS = combine_dicts(RunnerOptionStore.COMBINERS, {
        'qubole_bootstrap': combine_lists,
        'bootstrap_actions': combine_lists,
        'bootstrap_cmds': combine_lists,
        'bootstrap_files': combine_path_lists,
        'bootstrap_python_packages': combine_path_lists,
        'bootstrap_scripts': combine_path_lists,
        'ec2_key_pair_file': combine_paths,
        's3_log_uri': combine_paths,
        's3_scratch_uri': combine_paths,
        'ssh_bin': combine_cmds,
        #'emr_api_params': combine_dicts
    })

    def __init__(self, alias, opts, conf_path):
        super(QuboleRunnerOptionStore, self).__init__(alias, opts, conf_path)

    def default_options(self):
        super_opts = super(QuboleRunnerOptionStore, self).default_options()
        return combine_dicts(super_opts, {
            #'ami_version': 'latest',
            #'check_emr_status_every': 30,
            #'cleanup_on_failure': ['JOB'],
            #'hadoop_version': None,
        })


class QuboleJobRunner(MRJobRunner):
    """Runs an :py:class:`~mrjob.job.MRJob` on Amazon Elastic MapReduce.
    Invoked when you run your job with ``-r qubole``.

    :py:class:`QuboleJobRunner` runs your job in Qubole as a Qubole Workflow Command.

    Input, support, and jar files can be either local or on S3; use
    ``s3://...`` URLs to refer to files on S3.

    This class has some useful utilities for talking directly to S3 and EMR,
    so you may find it useful to instantiate it without a script::
    """
    alias = 'qubole'

    # Don't need to bootstrap mrjob in the setup wrapper; that's what
    # the bootstrap script is for!
    BOOTSTRAP_MRJOB_IN_SETUP = False

    OPTION_STORE_CLASS = QuboleRunnerOptionStore

    def __init__(self, **kwargs):
        """
        <TODO: Comments>
        """
        super(QuboleJobRunner, self).__init__(**kwargs)

        # make aws_region an instance variable; we might want to set it
        # based on the scratch bucket
        self._aws_region = self._opts['aws_region'] or ''

        # if we're going to create a bucket to use as temp space, we don't
        # want to actually create it until we run the job (Issue #50).
        # This variable helps us create the bucket as needed
        self._s3_temp_bucket_to_create = None

        self._fix_s3_scratch_and_log_uri_opts()

        # pick a tmp dir based on the job name
        self._s3_tmp_uri = self._opts['s3_scratch_uri'] + self._job_name + '/'

        # pick/validate output dir
        if self._output_dir:
            self._output_dir = self._check_and_fix_s3_dir(self._output_dir)
        else:
            self._output_dir = self._s3_tmp_uri + 'output/'

        # # manage working dir for bootstrap script
        self._bootstrap_dir_mgr = BootstrapWorkingDirManager() #adding because of setup wrapper

        # manage local files that we want to upload to S3. We'll add them
        # to this manager just before we need them.
        s3_files_dir = self._s3_tmp_uri + 'files/'
        self._upload_mgr = UploadDirManager(s3_files_dir)

        # Code commented out because we are not creating a bootstrap script. Instead, the --qubole-bootstrap option will work.
        # # add the bootstrap files to a list of files to upload
        # self._bootstrap_actions = []
        # for action in self._opts['bootstrap_actions']:
        #     args = shlex_split(action)
        #     if not args:
        #         raise ValueError('bad bootstrap action: %r' % (action,))
        #     # don't use _add_bootstrap_file() because this is a raw bootstrap
        #     self._bootstrap_actions.append({
        #         'path': args[0],
        #         'args': args[1:],
        #     })

        # for path in self._opts['bootstrap_files']:
        #     self._bootstrap_dir_mgr.add(**parse_legacy_hash_path(
        #         'file', path, must_name='bootstrap_files'))

        self._bootstrap = self._parse_bootstrap()
        # # self._legacy_bootstrap = self._parse_legacy_bootstrap()

        # for cmd in self._bootstrap + self._legacy_bootstrap:
        #     for maybe_path_dict in cmd:
        #         if isinstance(maybe_path_dict, dict):
        #             self._bootstrap_dir_mgr.add(**maybe_path_dict)

        # where our own logs ended up (we'll find this out once we run the job)
        self._s3_job_log_uri = None

        # we'll create the script later
        self._master_bootstrap_script_path = None

        # the ID assigned by EMR to this job (might be None)
        self._emr_job_flow_id = self._opts['emr_job_flow_id']

        # when did our particular task start?
        self._qubole_job_start = None

        # ssh state
        self._ssh_proc = None
        self._gave_cant_ssh_warning = False
        # we don't upload the ssh key to master until it's needed
        self._ssh_key_is_copied = False

        # cache for SSH address
        self._address = None
        self._ssh_slave_addrs = None

        # store the tracker URL for completion status
        self._tracker_url = None

        # turn off tracker progress until tunnel is up
        self._show_tracker_progress = False

        # init hadoop version cache
        self._inferred_hadoop_version = None

    def _fix_s3_scratch_and_log_uri_opts(self):
        """Fill in s3_scratch_uri and s3_log_uri (in self._opts) if they
        aren't already set.

        Helper for __init__.
        """
        s3_conn = self.make_s3_conn()
        # check s3_scratch_uri against aws_region if specified
        if self._opts['s3_scratch_uri']:
            bucket_name, _ = parse_s3_uri(self._opts['s3_scratch_uri'])
            bucket_loc = s3_conn.get_bucket(bucket_name).get_location()

            # make sure they can communicate if both specified
            if (self._aws_region and bucket_loc and
                    self._aws_region != bucket_loc):
                log.warning('warning: aws_region (%s) does not match bucket'
                            ' region (%s). Your EC2 instances may not be able'
                            ' to reach your S3 buckets.' %
                            (self._aws_region, bucket_loc))

            # otherwise derive aws_region from bucket_loc
            elif bucket_loc and not self._aws_region:
                log.info(
                    "inferring aws_region from scratch bucket's region (%s)" %
                    bucket_loc)
                self._aws_region = bucket_loc
        # set s3_scratch_uri by checking for existing buckets
        else:
            self._set_s3_scratch_uri(s3_conn)
            log.info('using %s as our scratch dir on S3' %
                     self._opts['s3_scratch_uri'])

        self._opts['s3_scratch_uri'] = self._check_and_fix_s3_dir(
            self._opts['s3_scratch_uri'])

        # set s3_log_uri
        if self._opts['s3_log_uri']:
            self._opts['s3_log_uri'] = self._check_and_fix_s3_dir(
                self._opts['s3_log_uri'])
        else:
            self._opts['s3_log_uri'] = self._opts['s3_scratch_uri'] + 'logs/'

    def _set_s3_scratch_uri(self, s3_conn):
        """Helper for _fix_s3_scratch_and_log_uri_opts"""
        buckets = s3_conn.get_all_buckets()
        mrjob_buckets = [b for b in buckets if b.name.startswith('mrjob-')]

        # Loop over buckets until we find one that is not region-
        #   restricted, matches aws_region, or can be used to
        #   infer aws_region if no aws_region is specified
        for scratch_bucket in mrjob_buckets:
            scratch_bucket_name = scratch_bucket.name
            scratch_bucket_location = scratch_bucket.get_location()

            if scratch_bucket_location:
                if scratch_bucket_location == self._aws_region:
                    # Regions are both specified and match
                    log.info("using existing scratch bucket %s" %
                             scratch_bucket_name)
                    self._opts['s3_scratch_uri'] = (
                        's3://%s/tmp/' % scratch_bucket_name)
                    return
                elif not self._aws_region:
                    # aws_region not specified, so set it based on this
                    #   bucket's location and use this bucket
                    self._aws_region = scratch_bucket_location
                    log.info("inferring aws_region from scratch bucket's"
                             " region (%s)" % self._aws_region)
                    self._opts['s3_scratch_uri'] = (
                        's3://%s/tmp/' % scratch_bucket_name)
                    return
                elif scratch_bucket_location != self._aws_region:
                    continue
            elif not self._aws_region:
                # Only use regionless buckets if the job flow is regionless
                log.info("using existing scratch bucket %s" %
                         scratch_bucket_name)
                self._opts['s3_scratch_uri'] = (
                    's3://%s/tmp/' % scratch_bucket_name)
                return

        # That may have all failed. If so, pick a name.
        scratch_bucket_name = 'mrjob-%016x' % random.randint(0, 2 ** 64 - 1)
        self._s3_temp_bucket_to_create = scratch_bucket_name
        log.info("creating new scratch bucket %s" % scratch_bucket_name)
        self._opts['s3_scratch_uri'] = 's3://%s/tmp/' % scratch_bucket_name

    # The function below looked unnecessary and hence has been commented-out
    # def _set_s3_job_log_uri(self, job_flow):
    #     """Given a job flow description, set self._s3_job_log_uri. This allows
    #     us to call self.ls(), etc. without running the job.
    #     """
    #     log_uri = getattr(job_flow, 'loguri', '')
    #     if log_uri:
    #         self._s3_job_log_uri = '%s%s/' % (
    #             log_uri.replace('s3n://', 's3://'), self._emr_job_flow_id)

    def _create_s3_temp_bucket_if_needed(self):
        """Make sure temp bucket exists"""
        if self._s3_temp_bucket_to_create:
            s3_conn = self.make_s3_conn()
            log.info('creating S3 bucket %r to use as scratch space' %
                     self._s3_temp_bucket_to_create)
            location = s3_location_constraint_for_region(self._aws_region)
            s3_conn.create_bucket(
                self._s3_temp_bucket_to_create, location=location)
            self._s3_temp_bucket_to_create = None

    def _check_and_fix_s3_dir(self, s3_uri):
        """Helper for __init__"""
        if not is_s3_uri(s3_uri):
            raise ValueError('Invalid S3 URI: %r' % s3_uri)
        if not s3_uri.endswith('/'):
            s3_uri = s3_uri + '/'

        return s3_uri

    @property
    def _ssh_key_name(self):
        return self._job_name + '.pem'

    @property
    def fs(self):
        """:py:class:`~mrjob.fs.base.Filesystem` object for SSH, S3, and the
        local filesystem.
        """
        if self._fs is None:
            if self._opts['s3_endpoint']:
                s3_endpoint = self._opts['s3_endpoint']
            else:
                s3_endpoint = s3_endpoint_for_region(self._aws_region)

            self._s3_fs = S3Filesystem(self._opts['aws_access_key_id'],
                                       self._opts['aws_secret_access_key'],
                                       s3_endpoint)

            if self._opts['ec2_key_pair_file']:
                self._ssh_fs = SSHFilesystem(self._opts['ssh_bin'],
                                             self._opts['ec2_key_pair_file'],
                                             self._ssh_key_name)
                self._fs = CompositeFilesystem(self._ssh_fs, self._s3_fs,
                                               LocalFilesystem())
            else:
                self._ssh_fs = None
                self._fs = CompositeFilesystem(self._s3_fs, LocalFilesystem())

        return self._fs

    def _run(self):
        self._prepare_for_launch()
        qubole_command = self._launch_qubole_job()
        self._wait_for_job_to_complete(qubole_command)

    def _prepare_for_launch(self):
        self._check_input_exists()
        self._check_output_not_exists()
        self._create_setup_wrapper_script() # To get --setup version working
        self._add_bootstrap_files_for_upload()
        self._add_job_files_for_upload()
        self._upload_local_files_to_s3()

    def _check_input_exists(self):
        """Make sure all input exists before continuing with our job.
        """
        for path in self._input_paths:
            if path == '-':
                continue  # STDIN always exists

            if is_uri(path) and not is_s3_uri(path):
                continue  # can't check non-S3 URIs, hope for the best

            if not self.path_exists(path):
                raise AssertionError(
                    'Input path %s does not exist!' % (path,))

    def _check_output_not_exists(self):
        """Verify the output path does not already exist. This avoids
        provisioning a cluster only to have Hadoop refuse to launch.
        """
        try:
            if self.fs.path_exists(self._output_dir):
                raise IOError(
                    'Output path %s already exists!' % (self._output_dir,))
        except boto.exception.S3ResponseError:
            pass

    def _add_bootstrap_files_for_upload(self, persistent=False):
        """Add files needed by the bootstrap script to self._upload_mgr.

        Tar up mrjob if bootstrap_mrjob is True.

        Create the master bootstrap script if necessary.

        persistent -- set by make_persistent_job_flow()
        """
        # lazily create mrjob.tar.gz
        # if self._opts['bootstrap_mrjob']:
        #     self._create_mrjob_tar_gz()
        #     self._bootstrap_dir_mgr.add('file', self._mrjob_tar_gz_path)

        # all other files needed by the script are already in
        # _bootstrap_dir_mgr
        for path in self._bootstrap_dir_mgr.paths():
            self._upload_mgr.add(path)

        # # now that we know where the above files live, we can create
        # # the master bootstrap script
        # self._create_master_bootstrap_script_if_needed()
        # if self._master_bootstrap_script_path:
        #     self._upload_mgr.add(self._master_bootstrap_script_path)

        # # make sure bootstrap action scripts are on S3
        # for bootstrap_action in self._bootstrap_actions:
        #     self._upload_mgr.add(bootstrap_action['path'])

        # # Add max-hours-idle script if we need it
        # if (self._opts['max_hours_idle'] and
        #         (persistent or self._opts['pool_emr_job_flows'])):
        #     self._upload_mgr.add(_MAX_HOURS_IDLE_BOOTSTRAP_ACTION_PATH)

    def _add_job_files_for_upload(self):
        """Add files needed for running the job (setup and input)
        to self._upload_mgr."""
        for path in self._get_input_paths():
            self._upload_mgr.add(path)

        for path in self._working_dir_mgr.paths():
            self._upload_mgr.add(path)

        if self._opts['hadoop_streaming_jar']:
            self._upload_mgr.add(self._opts['hadoop_streaming_jar']) #this bug was also there on emr.py, not fixed there. This option is used for adding a custom JAR in Hadoop Streaming

        for step in self._get_steps():
            if step.get('jar'):
                self._upload_mgr.add(step['jar'])

    def _upload_local_files_to_s3(self):
        """Copy local files tracked by self._upload_mgr to S3."""
        self._create_s3_temp_bucket_if_needed()

        log.info('Copying non-input files into %s' % self._upload_mgr.prefix)

        s3_conn = self.make_s3_conn()

        for path, s3_uri in self._upload_mgr.path_to_uri().iteritems():
            log.debug('uploading %s -> %s' % (path, s3_uri))
            s3_key = self.make_s3_key(s3_uri, s3_conn)
            s3_key.set_contents_from_filename(path)

    ### Running the job ###

    def cleanup(self, mode=None):
        super(QuboleJobRunner, self).cleanup(mode=mode)

    def _cleanup_remote_scratch(self):
        # delete all the files we created
        if self._s3_tmp_uri:
            try:
                log.info('Removing all files in %s' % self._s3_tmp_uri)
                self.rm(self._s3_tmp_uri)
                self._s3_tmp_uri = None
            except Exception, e:
                log.exception(e)

    # function looked unused, hence removed. Maybe a similar function can be added in future when logs are handled in a better way
    # def _cleanup_logs(self):
    #     super(QuboleJobRunner, self)._cleanup_logs()

    #     # delete the log files, if it's a job flow we created (the logs
    #     # belong to the job flow)
    #     if self._s3_job_log_uri and not self._opts['emr_job_flow_id'] \
    #             and not self._opts['pool_emr_job_flows']:
    #         try:
    #             log.info('Removing all files in %s' % self._s3_job_log_uri)
    #             self.rm(self._s3_job_log_uri)
    #             self._s3_job_log_uri = None
    #         except Exception, e:
    #             log.exception(e)

    def _cleanup_job(self):
        # kill the job if we won't be taking down the whole job flow
        if not (self._emr_job_flow_id or
                self._opts['emr_job_flow_id'] or
                self._opts['pool_emr_job_flows']):
            # we're taking down the job flow, don't bother
            return

        error_msg = ('Unable to kill job without terminating job flow and'
                     ' job is still running. You may wish to terminate it'
                     ' yourself with "python -m mrjob.tools.emr.terminate_job_'
                     'flow %s".' % self._emr_job_flow_id)

        try:
            addr = self._address_of_master()
        except IOError:
            return

        if not self._ran_job:
            try:
                log.info("Attempting to terminate job...")
                had_job = ssh_terminate_single_job(
                    self._opts['ssh_bin'],
                    addr,
                    self._opts['ec2_key_pair_file'])
                if had_job:
                    log.info("Succeeded in terminating job")
                else:
                    log.info("Job appears to have already been terminated")
            except IOError:
                log.info(error_msg)

    def _wait_for_s3_eventual_consistency(self):
        """Sleep for a little while, to give S3 a chance to sync up.
        """
        log.info('Waiting %.1fs for S3 eventual consistency' %
                 self._opts['s3_sync_wait_time'])
        time.sleep(self._opts['s3_sync_wait_time'])

    def _job_flow_is_done(self, job_flow):
        return job_flow.state in ('TERMINATED', 'COMPLETED', 'FAILED',
                                  'SHUTTING_DOWN')

    def _build_steps(self):
        """Return a list of boto Step objects corresponding to the
        steps we want to run."""
        # quick, add the other steps before the job spins up and
        # then shuts itself down (in practice this takes several minutes)
        steps = [self._build_step(n) for n in xrange(self._num_steps())]
        if self._bootstrap:
            steps.insert(0,self._build_shell_step(self._bootstrap))
        return steps

    def _build_step(self, step_num):
        step = self._get_step(step_num)

        if step['type'] == 'streaming':
            return self._build_streaming_step(step_num)
        elif step['type'] == 'jar':
            return self._build_jar_step(step_num)
        else:
            raise AssertionError('Bad step type: %r' % (step['type'],))

    def _build_streaming_step(self, step_num):
        if (self._opts['hadoop_streaming_jar']): #Fixed custom JAR
            wf_step_args = 'jar ' + self._upload_mgr.uri(self._opts['hadoop_streaming_jar']) + ' '
        else:
            wf_step_args = 'streaming '

        streaming_step_kwargs = {
            'name': '%s: Step %d of %d' % (
                self._job_name, step_num + 1, self._num_steps()),
            'input': self._step_input_uris(step_num),
            'output': self._step_output_uri(step_num),
        }

        streaming_step_kwargs.update(self._cache_kwargs())

        streaming_step_kwargs['step_args'].extend(
            self._hadoop_args_for_step(step_num))

        mapper, combiner, reducer = (
            self._hadoop_streaming_commands(step_num))

        streaming_step_kwargs['mapper'] = mapper

        if combiner:
            streaming_step_kwargs['combiner'] = combiner

        streaming_step_kwargs['reducer'] = reducer

        step_args_string = ' '.join(streaming_step_kwargs['step_args'])

        input_string = ' '.join(streaming_step_kwargs['input'])

        wf_step_args = wf_step_args + step_args_string + \
                    ' -input ' + input_string + \
                    ' -output ' + streaming_step_kwargs['output'] + \
                    ' -mapper ' + "'" + streaming_step_kwargs['mapper'] + "'" + \
                    ' -reducer ' + "'" + streaming_step_kwargs['reducer'] + "'"
        if combiner:
            wf_step_args = wf_step_args + ' -combiner ' + "'" + streaming_step_kwargs['combiner'] + "'"

        return HadoopCommand.parse(shlex_split(wf_step_args))

    def _build_shell_step(self, shell_cmd):
        final_cmd = "'"
        for cmd in shell_cmd:
            for token in cmd:
                final_cmd += token + ";"
        final_cmd += "'"
        wf_step_args = '--script=' + final_cmd
        return ShellCommand.parse(shlex_split(wf_step_args))

    def _build_jar_step(self, step_num):
        step = self._get_step(step_num)

        # special case to allow access to jars inside EMR
        if step['jar'].startswith('file:///'):
            jar = step['jar'][7:]  # keep leading slash
        else:
            jar = self._upload_mgr.uri(step['jar'])

        def interpolate(arg):
            if arg == mrjob.step.JarStep.INPUT:
                return ','.join(self._step_input_uris(step_num))
            elif arg == mrjob.step.JarStep.OUTPUT:
                return self._step_output_uri(step_num)
            else:
                return arg

        step_args = step['args']
        if step_args:
            step_args = [interpolate(arg) for arg in step_args]

        wf_step_args = 'jar '
        wf_step_args += jar + ' '
        for args in step_args:
            wf_step_args += args + ' '
        return HadoopCommand.parse(shlex_split(wf_step_args)) #This fixes all JAR Steps

    def _cache_kwargs(self):
        step_args = []
        cache_files = []
        cache_archives = []

        step_args.extend(self._new_upload_args(self._upload_mgr))

        return {
            'step_args': step_args,
            # Qubole: Not sure if we need or can use these
            # 'cache_files': cache_files,
            # 'cache_archives': cache_archives,
        }


    def _configure_qubole_connection(self):
        Qubole.configure(api_token=self._opts['api_token'], version=self._opts['api_version'])

    def _launch_qubole_job(self):
        """Create ..."""
        self._create_s3_temp_bucket_if_needed()
        self._configure_qubole_connection()
        steps = self._build_steps()

        """ Creating a Qubole Workflow Command (aka: CompositeCommand) ... """
        composite = CompositeCommand.compose(steps, cluster_label=self._opts['cluster_label'], notify=False, name=self._job_name)
        composite_command = CompositeCommand.create(**composite)
        # CompositeCommand.run(**composite)
        self._qubole_job_start = time.time()
        log.info("Starting Qubole Command: %s" % composite_command.id)
        log.info("Qubole UI Link: %s" % QUBOLE_ANALYZE_URL+str(composite_command.id))
        return composite_command

    def _wait_for_job_to_complete(self, composite_command):
        """Wait for the job to complete, and raise an exception if
        the job failed.

        """
        # grab the command ID, poll for status
        # Print results and run time
        # Consider allowing verbose flag to grab logs (or maybe we just print the logs as we poll...)
        gcount=0
        lcount=0
        while True:
            sts = HiveCommand.find(composite_command.id).status
            if sts == u'error' or sts == u'done' or sts == u'cancelled':
                break
            time.sleep(QUBOLE_POLL_TIME)
            # Print the logs to the console
            qlog=CompositeCommand.find(composite_command.id).get_log()
            str=qlog.split('\n')
            lcount=0
            for line in str:
                if lcount < gcount:
                    lcount = lcount+1
                    continue
                log.info(line)
                lcount = lcount+1
                gcount = gcount+1

        if sts != u'done':
            error_str = composite_command.get_log()
            sys.stderr.write(error_str)
            raise QuboleError(error_str)
        composite_command.get_results()



    def _step_input_uris(self, step_num):
        """Get the s3:// URIs for input for the given step."""
        if step_num == 0:
            return [self._upload_mgr.uri(path)
                    for path in self._get_input_paths()]
        else:
            # put intermediate data in HDFS
            return ['hdfs:///tmp/mrjob/%s/step-output/%s/' % (
                self._job_name, step_num)]

    def _step_output_uri(self, step_num):
        if step_num == len(self._get_steps()) - 1:
            return self._output_dir
        else:
            # put intermediate data in HDFS
            return 'hdfs:///tmp/mrjob/%s/step-output/%s/' % (
                self._job_name, step_num + 1)

    ### Bootstrapping ###
    # Bootstrap script is not created. Instead, a shell command is executed as first step in workflow if we use the --qubole-bootstrap command
    # def _create_master_bootstrap_script_if_needed(self):
    #     """Helper for :py:meth:`_add_bootstrap_files_for_upload`.

    #     Create the master bootstrap script and write it into our local
    #     temp directory. Set self._master_bootstrap_script_path.

    #     This will do nothing if there are no bootstrap scripts or commands,
    #     or if it has already been called."""
    #     if self._master_bootstrap_script_path:
    #         return

    #     # don't bother if we're not starting a job flow
    #     if self._opts['emr_job_flow_id']:
    #         return

    #     # Also don't bother if we're not bootstrapping
    #     if not (self._bootstrap or self._legacy_bootstrap or
    #             self._opts['bootstrap_files']
    #             or self._opts['bootstrap_mrjob']):
    #         return

    #     # create mrjob.tar.gz if we need it, and add commands to install it
    #     mrjob_bootstrap = []
    #     if self._opts['bootstrap_mrjob']:
    #         # _add_bootstrap_files_for_upload() should have done this
    #         assert self._mrjob_tar_gz_path
    #         path_dict = {
    #             'type': 'file', 'name': None, 'path': self._mrjob_tar_gz_path}
    #         self._bootstrap_dir_mgr.add(**path_dict)

    #         # find out where python keeps its libraries
    #         mrjob_bootstrap.append([
    #             "__mrjob_PYTHON_LIB=$(%s -c "
    #             "'from distutils.sysconfig import get_python_lib;"
    #             " print get_python_lib()')" %
    #             cmd_line(self._opts['python_bin'])])
    #         # un-tar mrjob.tar.gz
    #         mrjob_bootstrap.append(
    #             ['sudo tar xfz ', path_dict, ' -C $__mrjob_PYTHON_LIB'])
    #         # re-compile pyc files now, since mappers/reducers can't
    #         # write to this directory. Don't fail if there is extra
    #         # un-compileable crud in the tarball (this would matter if
    #         # sh_bin were 'sh -e')
    #         mrjob_bootstrap.append(
    #             ['sudo %s -m compileall -f $__mrjob_PYTHON_LIB/mrjob && true' %
    #              cmd_line(self._opts['python_bin'])])

    #     # we call the script b.py because there's a character limit on
    #     # bootstrap script names (or there was at one time, anyway)
    #     path = os.path.join(self._get_local_tmp_dir(), 'b.py')
    #     log.info('writing master bootstrap script to %s' % path)

    #     contents = self._master_bootstrap_script_content(
    #         self._bootstrap + mrjob_bootstrap + self._legacy_bootstrap)
    #     for line in StringIO(contents):
    #         log.debug('BOOTSTRAP: ' + line.rstrip('\r\n'))

    #     with open(path, 'w') as f:
    #         f.write(contents)

    #     self._master_bootstrap_script_path = path

    def _parse_bootstrap(self):
        """Parse the *bootstrap* option with
        :py:func:`mrjob.setup.parse_setup_cmd()`.
        """
        return [parse_setup_cmd(cmd) for cmd in self._opts['qubole_bootstrap']]

    # def _parse_legacy_bootstrap(self):
    #     """Parse the deprecated
    #     options *bootstrap_python_packages*, and *bootstrap_cmds*
    #     *bootstrap_scripts* as bootstrap commands, in that order.

    #     This is a separate method from _parse_bootstrap() because bootstrapping
    #     mrjob happens after the new bootstrap commands (so you can upgrade
    #     Python) but before the legacy commands (for backwards compatibility).
    #     """
    #     bootstrap = []

    #     # bootstrap_python_packages
    #     if self._opts['bootstrap_python_packages']:
    #         # 3.0.x AMIs use yum rather than apt-get;
    #         # can't determine which AMI `latest` is at
    #         # job flow creation time so we call both
    #         bootstrap.append(['sudo apt-get install -y python-pip || '
    #             'sudo yum install -y python-pip'])

    #     for path in self._opts['bootstrap_python_packages']:
    #         path_dict = parse_legacy_hash_path('file', path)
    #         # don't worry about inspecting the tarball; pip is smart
    #         # enough to deal with that
    #         bootstrap.append(['sudo pip install ', path_dict])

    #     # setup_cmds
    #     for cmd in self._opts['bootstrap_cmds']:
    #         if not isinstance(cmd, basestring):
    #             cmd = cmd_line(cmd)
    #         bootstrap.append([cmd])

    #     # bootstrap_scripts
    #     for path in self._opts['bootstrap_scripts']:
    #         path_dict = parse_legacy_hash_path('file', path)
    #         bootstrap.append([path_dict])

    #     return bootstrap

    # def _master_bootstrap_script_content(self, bootstrap):
    #     """Create the contents of the master bootstrap script.
    #     """
    #     out = StringIO()

    #     def writeln(line=''):
    #         out.write(line + '\n')

    #     # shebang
    #     sh_bin = self._opts['sh_bin']
    #     if not sh_bin[0].startswith('/'):
    #         sh_bin = ['/usr/bin/env'] + sh_bin
    #     writeln('#!' + cmd_line(sh_bin))
    #     writeln()

    #     # store $PWD
    #     writeln('# store $PWD')
    #     writeln('__mrjob_PWD=$PWD')
    #     writeln()

    #     # download files using hadoop fs
    #     writeln('# download files and mark them executable')
    #     for name, path in sorted(
    #             self._bootstrap_dir_mgr.name_to_path('file').iteritems()):
    #         uri = self._upload_mgr.uri(path)
    #         writeln('hadoop fs -copyToLocal %s $__mrjob_PWD/%s' %
    #                 (pipes.quote(uri), pipes.quote(name)))
    #         # make everything executable, like Hadoop Distributed Cache
    #         writeln('chmod a+x $__mrjob_PWD/%s' % pipes.quote(name))
    #     writeln()

    #     # run bootstrap commands
    #     writeln('# bootstrap commands')
    #     for cmd in bootstrap:
    #         # reconstruct the command line, substituting $__mrjob_PWD/<name>
    #         # for path dicts
    #         line = ''
    #         for token in cmd:
    #             if isinstance(token, dict):
    #                 # it's a path dictionary
    #                 line += '$__mrjob_PWD/'
    #                 line += pipes.quote(self._bootstrap_dir_mgr.name(**token))
    #             else:
    #                 # it's raw script
    #                 line += token
    #         writeln(line)
    #     writeln()

    #     return out.getvalue()

    def get_hadoop_version(self):
        return QUBOLE_HADOOP_VERSION
