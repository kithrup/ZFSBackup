from __future__ import print_function
import os, sys
import subprocess
import time
import tempfile
import threading

debug = True

def _merge_snapshots(list1, list2):
    """
    Given a list of snapshots, return a list of
    common snapshots (sorted by creation time).
    The return list is simply an array of names.
    N.B.: Snapshots are assumed to be the same if
    they have the same name!
    """
    rv = []
    if list2:
        dict2 = dict((el["Name"], True) for el in list2)
        for snapname in [x["Name"] for x in list1]:
            if snapname in dict2:
                rv.append(snapname)
            else:
                pass;
    return rv

def CHECK_OUTPUT(*args, **kwargs):
    if debug:
        print("CHECK_OUTPUT({}, {})".format(args, kwargs), file=sys.stderr)
    return subprocess.check_output(*args, **kwargs)

def CALL(*args, **kwargs):
    if debug:
        print("CALL({}, {})".format(args, kwargs, file=sys.stderr))
    return subprocess.call(*args, **kwargs)

def CHECK_CALL(*args, **kwargs):
    if debug:
        print("CHECK_CALL({}, {})".format(args, kwargs), file=sys.stderr)
    return subprocess.check_call(*args, **kwargs)

def POPEN(*args, **kwargs):
    if debug:
        print("POPEN({}, {})".format(args, kwargs), file=sys.stderr)
    return subprocess.Popen(*args, **kwargs)

def _get_snapshots(ds):
    """
    Return a list of snapshots for the given dataset.
    This only works for local ZFS pools, obviously.
    It relies on /sbin/zfs sorting, rather than sorting itself.
    """
    command = ["/sbin/zfs", "list", "-H", "-p", "-o", "name,creation",
               "-r", "-d", "1", "-t", "snapshot", "-s", "creation",
               ds]
    if debug:
        print("get_snapshots: {}".format(" ".join(command)), file=sys.stderr)
    try:
        output = CHECK_OUTPUT(command).split("\n")
    except subprocess.CalledProcessError:
        # We'll assume this is because there are no snapshots
        return []
    snapshots = []
    for snapshot in output:
        if not snapshot:
            continue
        (name, ctime) = snapshot.rstrip().split()
        name = name.split('@')[1]
        snapshots.append({"Name" : name, "CreationTime" : int(ctime) })
    return snapshots

class ZFSBackupError(ValueError):
    pass

class ZFSBackupFilter(object):
    """
    Base class for ZFS backup filters.
    Filters have several properties, and
    start_backup() and start_restore() methods.
    The start_* methods take a source, which
    should be a pipe.  In general, the filters
    should use a subprocess or thread, unless they
    are the terminus of the pipeline.  (Doing otherwise
    risks deadlock.)
    """
    def __init__(self):
        pass

    @property
    def error_output(self):
        return None
    @error_output.setter
    def error_output(self, e):
        return
    
    @property
    def name(self):
        return "Null Filter"

    @property
    def backup_command(self):
        return []
    @property
    def restore_command(self):
        return []
    
    def start_backup(self, source):
        """
        Start the filter when doing a backup.
        E.g., for a compression filter, this would
        start the command (in a subprocess) to
        run gzip.
        """
        return source
    def start_restore(self, source):
        """
        Start the filter when doing a restore.
        E.g., for a compression filter, this would
        start the command (in a subprocess) to
        run 'gzcat'.
        """
        return source
    
class ZFSBackupFilterThread(ZFSBackupFilter, threading.Thread):
    """
    Base class for a thread-based filter.  Either it should be
    subclassed (see ZFSBackupFilterCounter below), or it should
    be called with a callable object as the "process=" parameter.
    The process method may need to check ZFSBackupFilterThread.mode
    to decide if it is backing up or restoring.
    """
    def __init__(self, process=None, name="Thread Filter"):
        super(ZFSBackupFilterThread, self).__init__()
        threading.Thread.__init__(self)
        (self.input_pipe, self.output_pipe) = os.pipe()
        self._source = None
        self._done = threading.Event()
        self._done.clear()
        self._process = process
        if self._process is None:
            self._name = "Null Thread Filter"
        else:
            self._name = name

    @property
    def backup_command(self):
        return ["<thread>"]
    @property
    def restore_command(self):
        return ["<thread>"]
    
    @property
    def input_pipe(self):
        return self._input
    @input_pipe.setter
    def input_pipe(self, p):
        self._input = p
    @property
    def output_pipe(self):
        return self._output
    @output_pipe.setter
    def output_pipe(self, p):
        self._output = p
    @property
    def source(self):
        return self._source
    @property
    def mode(self):
        return self._mode
    
    def process(self, buf):
        # Subclasses should do any processing here
        if self._process:
            return self._process(buf)
    
    def run(self):
        while True:
            b = self.source.read(1024*1024)
            if b:
                os.write(self.output_pipe, self.process(b))
            else:
                break
        self._done.set()
        os.close(self.output_pipe)
        
    def start_backup(self, source):
        self._mode = "backup"
        self._source = source
        self._py_output = os.fdopen(self.input_pipe, "rb")
        self.start()
        return self._py_output

    def start_restore(self, source):
        self._mode = "restore"
        self._source = source
        rv = os.fdopen(self.input_pipe, "rb")
        self.start()
        return rv
    
class ZFSBackupFilterCommand(ZFSBackupFilter):
    """
    Derived class for backup filters based on commands.
    This adds a coupe properties, and starts the appropriate commands
    in a Popen instance.  The error parameter in the constructor is
    used to indicate where stderr should go; by default, it goes to
    /dev/null
    If restore_command is None, then backup_command will be used.
    """
    def __init__(self, backup_command=["/bin/cat"], restore_command=None,
                 error=None):
        super(ZFSBackupFilterCommand, self).__init__()
        self._backup_command=backup_command
        self._restore_command=restore_command
        self.error = error
        
    @property
    def backup_command(self):
        return self._backup_command
    @property
    def restore_command(self):
        return self._restore_command or self.backup_command
    @property
    def error_output(self):
        return self._error_output
    @error_output.setter
    def error_output(self, where):
        if self.error:
            self.error.close()
        self._error_output = where

    def start_restore(self, source):
        """
        source is a file-like object, usually a pipe.
        We run Popen, setting source as stdin, and
        subprocess.PIPE as stdout, and return popen.stdout.
        If error is None, we open /dev/null for writig and
        use that.
        """
        if self.error is None:
            self.error = open("/dev/null", "w+")
        p = POPEN(self.restore_command,
                  bufsize=1024 * 1024,
                  stdin=source,
                  stdout=subprocess.PIPE,
                  stderr=self.error)
        return p.stdout
    
    def start_backup(self, source):
        """
        source is a file-like object, usually a pipe.
        We run Popen, and setting source up as stdin,
        and subprocess.PIPE as output, and return
        popen.stdout.
        If error is None, we open /dev/null for writing
        and use that.
        """
        if self.error is None:
            self.error = open("/dev/null", "w+")
        p = POPEN(self.backup_command,
                  bufsize=1024 * 1024,
                  stderr=self.error,
                  stdin=source,
                  stdout=subprocess.PIPE)
        return p.stdout
    
class ZFSBackupFilterCompressed(ZFSBackupFilterCommand):
    """
    A sample command filter, for compressing.
    One optional parameter:  pigz.
    """
    def __init__(self, pigz=False):
        if pigz:
            backup_command = "/usr/local/bin/pigz"
            restore_command = "/usr/local/bin/unpigz"
        else:
            backup_command = "/usr/bin/gzip"
            restore_command = "/usr/bin/gunzip"
            
        super(ZFSBackupFilterCompressed, self).__init__(backup_command=[backup_command],
                                                        restore_command=[restore_command])
            
class ZFSBackupFilterCounter(ZFSBackupFilterThread):
    """
    A sample thread filter.  All this does is count the
    bytes that come in to be processed.
    """
    def __init__(self, handler=None):
        super(ZFSBackupFilterCounter, self).__init__()
        self._count = 0
        self.handler = handler
        
    def name(self):
        return "ZFS Count Filter"

    def process(self, b):
        self._count += len(b)
        return b

    @property
    def handler(self):
        return self._handler
    @handler.setter
    def handler(self, h):
        self._handler = h

    @property
    def count(self):
        self._done.wait()
        if self.handler and iscallable(self.handler):
            self.handler(self._count)
        return self._count

class ZFSBackup(object):
    """
    Base class for doing ZFS backups.
    Backups are done using snapshots -- zfs send is used -- not using files.
    Every backup must have a source and a target, although subclasses
    can change how they are interpreted.  Backups can be recursive.

    One ZFSBackup object should be created for each <source, target>, but
    not for each snapshot.  That is, you would use

    backup = ZFSBackup("/tank/Media", "/backup/tank/Media", recursive=True)
    <do backup>
    backup = ZFSBackup("/tank/Documents", "/backup/tank/Documents")
    <do backup>

    instead of creating a ZFSBackup object for each snapshot.

    In general, backups and restores are simply inverses of each other.

    In order to perform backups, it is necesary to get a list of snapshots
    on both the source and target.  An empty list on the target will mean
    a full backup is being done; an empty list on the source is a failure.

    Backups can have filters applied to them.  This is not used in the base
    class (since it only implements ZFS->ZFS), but subclasses may wish to
    add filters for compression, encryption, or accounting.  Some sample
    filter classes are provided.

    Some notes on how replication works:
    * source is the full path to the dataset. *Or* it can be the entire pool.
    * target is the dataset to which the replication should go.
    * If source is the full pool, then the target will have all of the files
    at the root of the source pool.
    * If source is NOT the full pool, then the target will end up with only the
    dataset(s) being replicated -- but any intervening datasets will be created.

    What this means:
    * tank -> backup/tank means we end up with backup/tank as a copy of tank.
    * tank/usr/home > backup/home means we end up with bakup/home/usr/home.
    * When getting snapshots for the destination, we need to add the path for
    source, *minus* the pool name.
    * UNLESS we are replicating the full pool.
    What *that* means:
    * tank -> backup/tank means getting snapshots from backup/tank
    * tanks/usr/home -> backup/home means getting snapshots from backup/home/usr/home

    """
    def __init__(self, source, target, recursive=False):
        """
        Parameters:
        source - (str) a ZFS pool or dataset to be backed up.
        target - (str) a ZFS dataset to be backed up.
        recursive - (bool) Indicate whether the backup is to be recursive or not.

        The only thing the base class does is run some validation tests
        on the source and target.
        """
        self.target = target
        self.source = source
        self.recursive = recursive
        self._source_snapshots = None
        self._target_snapshots = None
        self._filters = []
        self.validate()

    @property
    def target(self):
        return self._dest
    @target.setter
    def target(self, t):
        self._dest = t
    @property
    def source(self):
        return self._source
    @source.setter
    def source(self, s):
        self._source = s
        
    @property
    def recursive(self):
        return self._recursive
    @recursive.setter
    def recursive(self, b):
        self._recursive = b
        
    def AddFilter(self, filter):
        """
        Add a filter.  The filter is set up during the backup and
        restore methods.  The filter needs to be an instance of
        ZFSFilter -- at least, it needs to have the start_backup and
        start_restore methods.
        """
        if not callable(getattr(filter, "start_backup", None)) and \
           not callable(getattr(filter, "start_restore", None)):
            raise ValueError("Incorrect type passed for filter")
        self._filters.append(filter)
        
    def _filter_backup(self, source, error=None):
        # Private method, to stitch the backup filters together.
        input = source
        for f in self._filters:
            f.error_output = error
            input = f.start_backup(input)
        return input
    
    def _filter_restore(self, source, error=None):
        # Private method, to stitch the restore filters together.
        input = source
        for f in self._filters:
            f.error_output = error
            input = f.start_restore(input)
        return input
    
    def __repr__(self):
        return "{}(source={}, target={})".format(self.__class__.__name__, self.source, self.target)
    
    @property
    def source_snapshots(self):
        """
        Return a list of snapshots on the source.  The return value is
        an array of dictionaries; the dictionaries have, at minimum, two
        elements:
		Name	-- (str) Snapshot name. The part that goes after the '@'
        	CreationTime -- (int) Time (in unix epoch seconds) the snapshot was created.
        Even if the recursive is true, this _only_ lists the snapshots for the
        source (recursive requires that the same snapshot exist on the descendents,
        or it doesn't get backed up).
        We cache this so we don't have to keep doing a list.
        """
        if not self._source_snapshots:
            self._source_snapshots = _get_snapshots(self.source)
        return self._source_snapshots

    @property
    def target_snapshots(self):
        """
        Return a list of snapshots on the target.  The return value is
        an array of dictionaries; the dictionaries have, at minimum, two
        elements:
		Name	-- (str) Snapshot name. The part that goes after the '@'
        	CreationTime -- (int) Time (in unix epoch seconds) the snapshot was created.
        Even if the recursive is true, this _only_ lists the snapshots for the
        target dataset.
        We cache this so we dont have to keep doing a list.
        """
        if not self._target_snapshots:
            # See the long discussion above about snapshots.
            (src_pool, _, src_ds) = self.source.partition("/")
            if src_ds:
                target_path = "{}/{}".format(self.target, src_ds)
            else:
                target_path = "{}/{}".format(self.target, src_pool)
                
            self._target_snapshots = _get_snapshots(target_path)
        return self._target_snapshots

    def validate(self):
        """
        Ensure the destination exists.  Derived classes will want
        to override this (probably).
        """
        command = ["/sbin/zfs", "list", "-H", self.target]
        try:
            with open("/dev/null", "w") as devnull:
                CHECK_CALL(command, stdout=devnull, stderr=devnull)
        except subprocess.CalledProcessError:
            raise ZFSBackupError("Target {} does not exist".format(self.target))
        if not self.source_snapshots:
            # A source with no snapshots cannot be backed up
            raise ZFSBackupError("Source {} does not have snapshots".format(self.source))
        
        return

    def backup_handler(self, stream, **kwargs):
        """
        Method called to write the backup to the target.  In the base class,
        this simply creates the necessary datasets on the target, and then
        creates a Popen subprocess for 'zfs recv' with the appropriate arguments,
        and sets its stdin to stream.
        Subclasses will probably want to replace this method.
        """
        # First we create the intervening dataset paths.  That is, the
        # equivalent of 'mkdir -p ${target}/${source}'.
        # We don't care if it fails.
        full_path = self.target
        with open("/dev/null", "w+") as devnull:
            for d in self.source.split("/")[1:]:
                full_path = os.path.join(full_path, d)
                command = ["/sbin/zfs", "create", "-o", "readonly=on", full_path]
                if debug:
                    print("Running command {}".format(" ".join(command)), file=sys.stderr)
                try:
                    CALL(command, stdout=devnull, stderr=devnull)
                except:
                    pass
        # Now we just send the data to zfs recv.
        # Do we need -p too?
        command = ["/sbin/zfs", "receive", "-d", "-F", self.target]
        with tempfile.TemporaryFile() as error_output:
            # ZFS->ZFS replication doesn't use filters.
            fobj = stream
            try:
                CHECK_CALL(command, stdin=fobj,
                                      stderr=error_output)
            except subprocess.CalledProcessError:
                error_output.seek(0)
                raise ZFSBackupError(error_output.read())
        return

    def backup(self, snapname=None, force_full=False):
        """
        Back up the source to the target.
        If snapname is given, then that will be the snapshot used for the backup,
        otherwise it will be the most recent snapshot.  If snapname is given and
        does not exist, an exception is raised.

        By default, it will first find a list of snapshots in common with the
        source and target, ordered chronologically (based on the source).

        If force_full is True, then the snapshot chosen will be sent in its entirety,
        rather than trying to find a common ancestor for an incremental snapshot.

        This is the main driver of the backup process, and subclasses should be okay
        with using it.

        """
        # First, if snapname is given, let's make sure that it exists on the source.
        if snapname:
            # If snapname has the dataset in it, let's remove it
            if '@' in snapname:
                (_, snapname) = snapname.split("@")
            snap_index = None

            for indx, d in enumerate(self.source_snapshots):
                if d["Name"] == snapname:
                    snap_index = indx
                    break
            if snap_index is None:
                raise ZFSBackupError("Specified snapshot {} does not exist".foramt(snapname))
            # We want to remove everything in source_snapshots up to the given one
            source_snapshots = self.source_snapshots[0:snap_index+1]
        else:
            source_snapshots = self.source_snapshots
            if debug:
                print("last_snapshot = {}".format(last_snapshot), file=sys.stderr)
            
        last_snapshot = source_snapshots[-1]
        last_common_snapshot = None
        if force_full:
            common_snapshots = []
        else:
            common_snapshots = _merge_snapshots(source_snapshots, self.target_snapshots)
        # At this point, common_snapshots has a list of snapshot names on both.
        # If there are no common snapshots, then we back up everything up to last_snapshot
        if debug:
            print("ZFSBackup: last_snapshot = {}, common_snapshots = {}".format(last_snapshot,
                                                                                common_snapshots),
                  file=sys.stderr)
        if last_snapshot["Name"] not in common_snapshots:
            if debug:
                print("We have to do some sends/receives", file=sys.stderr)
            # We need to do incremental snapshots from the last common snapshot to
            # last_snapshot.
            if common_snapshots:
                # Don't bother doing this if we have no snapshots in common
                last_common_snapshot = common_snapshots[-1]
                if debug:
                    print("Last common snapshot = {}".format(last_common_snapshot), file=sys.stderr)
                for indx, snap in enumerate(source_snapshots):
                    if snap["Name"] == last_common_snapshot:
                        break
                snapshot_list = source_snapshots[indx:]
            else:
                # Either it's been deleted on the remote end, or it's newer than the list.
                # So we start at a full dump from last_snapshot
                snapshot_list = [last_snapshot]
        else:
            snapshot_list = [last_snapshot]

        # There are two approaches that could be done here.
        # One is to do incremental sends for every snapshot; the other
        # is simply to do a send -I.  I'm choosing the latter.
        # If we have a last common snapshot, we can do an incremental from it to
        # the last snapshot; if we don't, we'll need to do a full send.
        command = ["/sbin/zfs", "send"]
        if self.recursive:
            command.append("-R")
        backup_dict = {}
        if last_common_snapshot:
            command.extend(["-I", "{}".format(last_common_snapshot)])
            backup_dict["incremental"] = True
            backup_dict["parent"] = last_common_snapshot
        else:
            backup_dict["incremental"] = False
        backup_dict["CreationTime"] = last_snapshot["CreationTime"]
        command.append("{}@{}".format(self.source, last_snapshot["Name"]))
        if debug:
            print(" ".join(command), file=sys.stderr)

        with tempfile.TemporaryFile() as error_output:
            with open("/dev/null", "w+") as devnull:
                mByte = 1024 * 1024
                send_proc = POPEN(command,
                                  bufsize=mByte,
                                  stdin=devnull,
                                  stderr=error_output,
                                  stdout=subprocess.PIPE)
                self.backup_handler(send_proc.stdout, **backup_dict)
            if send_proc.returncode:
                error_output.seek(0)
                raise ZFSBackupError(error_output.read())
        return

    def replicate(self, source, snapname, previous=None, date=int(time.time())):
        """
        Replicate from source.  source must be an object that supports
        read().  If date is not given, we will use the current time, so
        it should really be set.  The full snapshot name from the source
        would be dataset@snapname.  If previous is set, it indicates this
        is an incremental snapshot.

        The snapname, previous, and  date parameters are for informational purposes only;
        the base class doesn't use them, but derived classes may.
        """
        destination = os.path.join(self.target, self.dataset)
        command = ["/sbin/zfs", "receive", "-d", "-F", self.target]
        with tempfile.TemporaryFile() as error_output:
            # ZFS->ZFS replication doesn't use filters.
            # fobj = self._filter(source, error=error_output)
            fobj = source
            try:
                CHECK_CALL(command, stdin=fobj, stderr=error_output)
            except subprocess.CalledProcessError:
                name = "{}@{}".format(self.dataset, snapname)
                error_output.seek(0)
                if debug:
                    print("`{}` failed: {}".format(" ".join(command), error_output.read()),
                          file=sys.stderr)
                raise ZFSBackupError("Could not replicate {} to target {}".format(name, self.target))
        return
    
    @property
    def snapshots(self):
        """
        Return an array of snapshots for the destination.
        Each entry in the array is a dictonary with at least
        two keys -- Name and CreationTime.  CreationTime is
        an integer (unix seconds).  The array is sorted by
        creation time (oldest first).  If there are no snapshots,
        an empty array is returned.
        This would be better with libzfs.
        """
        command = ["/sbin/zfs", "list", "-H", "-p", "-o", "name,creation",
                   "-r", "-d", "1", "-t", "snapshot", "-s", "creation",
                   self.target]
        try:
            output = subprocess.check_output(command).split("\n")
        except subprocess.CalledProcessError:
            # We'll assume this is because there are no snapshots
            return []
        snapshots = []
        for snapshot in output:
            if not snapshot:
                continue
            (name, ctime) = snapshot.rstrip().split()
            snapshots.append({"Name" : name, "CreationTime" : int(ctime) })
        return snapshots

class ZFSBackupSSH(ZFSBackup):
    """
    Replicate to a remote host using ssh.
    This runs all of the commands the base class does, but via ssh
    to another host.

    When running a command on a remote host, we have the following
    options:
    1)  We don't care about input or output, only the return value.
    2)  We stream to it, or from it.

    (1) is mostly for validation -- ensure the target exists, and
    we can connect to it.
    For (2), we stream to it (writing to stdin), and don't care about
    the output until after, for backup.
    For (2), we stream _from_ it (reading from its stdout) when getting
    a list of snapshots, and when doing a restore.
    """
    def __init__(self, source, target, remote_host,
                 remote_user=None,
                 ssh_opts=[],
                 recursive=False):
        self._user = remote_user
        self._host = remote_host
        self._ssh_opts = ssh_opts[:]
        super(ZFSBackupSSH, self).__init__(source, target, recursive)

    @property
    def user(self):
        return self._user
    @property
    def host(self):
        return self._host
    @property
    def ssh_options(self):
        return self._ssh_opts

    def _build_command(self, cmd, *args):
        # First set up ssh.
        command = ["/usr/bin/ssh"]
        if self.ssh_options:
            command.extend(self.ssh_options)
        if self.user:
            command.append("{}@{}".format(self.user, self.host))
        else:
            command.append(self.host)
            
        # Then goes the rest of the command
        command.append(cmd)
        if args:
            command.extend(args)
        return command
    
    def _run_cmd(self, cmd, *args, **kwargs):
        """
        This implements running a command and not caring about
        the output.  If stdout or stderr are given, those will
        be file-like objects that the output and error are written
        to.  If the command exists with a non-0 value, we raise an
        exception.
        """
        command = self._build_command(cmd, *args)
        try:
            CHECK_CALL(command, **kwargs)
        except subprocess.CalledProcessError:
            raise ZFSBackupError("`{}` failed".format(command))

    def _remote_stream(self, cmd, *args, **kwargs):
        """
        Run a command on the remote host, but we want to write to or read
        from it.  We return a subprocess.Popen object, so the caller
        needs to specify stdin=subprocess.PIPE, or stdout.  Both can't be pipes.
        This should only be called by _remote_write or remote_stream
        """
        command = self._build_command(cmd, *args)
        return POPEN(cmd[0], *cmd[1:], **kwargs)
    
    def _remote_write(self, cmd, *args, **kwargs):
        """
        Run a command on the remote host, writing to it via stdin.
        """
        # First remove stdin=, if it's there.
        kwargs["stdin"] = subprocess.PIPE
        return self._remote_stream(cmd, *args, **kwargs)
    def _remote_read(self, cmd, *args, **kwargs):
        """
        Run a command on the remote host, reading its stdout.
        """
        # First remove stdout=, if it's there.
        kwargs["stdout"] = subprocess.PIPE
        return self._remote_stream(cmd, *args, **kwargs)

    def validate(self):
        """
        Do a couple of validations.
        """
        # See if we can connect to the remote host
        with tempfile.TemporaryFile() as error_output:
            try:
                self._run_cmd("/usr/bin/true", stderr=error_output)
            except ZFSBackupError:
                error_output.seek(0)
                raise ZFSBackupError("Unable to connect to remote host: {}".format(error_output.read()))
        # See if the target exists
        with open("/dev/null", "w+") as devnull:
            try:
                self._run_cmd("/sbin/zfs", "list", "-H", self.target,
                              stdout=devnull, stderr=devnull, stdin=devnull)
            except ZFSBackupError:
                raise ZFSBackupError("Target {} does not exist on remote host".format(self.target))

        return

    def backup_handler(self, stream, **kwargs):
        """
        Implement the replication.
        This is not right yet:  we need to decompress and decrypt and dewhatever else
        and do it by creating a pipeline on the remote end.
        """

        # First, we create the intervening dataset pats. See the base class' method.
        full_path = self.target
        with open("/dev/null", "w+") as devnull:
            for d in self.source.split("/")[1:]:
                full_path = os.path.join(full_path, d)
                command = self._build_command("/sbin/zfs", "create", "-o", "readonly=on", full_path)
                try:
                    CALL(command, stdout=devnull, stderr=devnull, stdin=devnull)
                except:
                    pass
                
        # Here's where we would have to go through the filters, if any, and undo them.
        # But some of the possible filters aren't needed, so I need a way to indicate that.
        # For now, I'll simply assume uncompressed, unencrypted, etc.
        command = self._build_command("/sbin/zfs", "receive", "-d", "-F", self.target)
        with tempfile.TemporaryFile() as error_output:
            # See above
            fobj = stream
            try:
                CHECK_CALL(command, stdin=fobj, stderr=error_output)
            except subprocess.CalledProcessError:
                error_output.seek(0)
                raise ZFSBackupError(error_output.read())

        return
    
    @property
    def target_snapshots(self):
        if not self._target_snapshots:
            (src_pool, _, src_ds) = self.source.partition("/")
            if src_ds:
                target_path = "{}/{}".format(self.target, src_ds)
            else:
                target_path = "{}/{}".format(self.target, src_pool)

            command = self._build_command("/sbin/zfs", "list", "-H", "-p",
                                          "-o", "name,creation", "-r",
                                          "-d", "1", "-t", "snapshot", "-s",
                                          "creation", target_path)
            snapshots = []
            try:
                output = CHECK_OUTPUT(command).split("\n")
                for snapshot in output:
                    if not snapshot:
                        continue
                    (name, ctime) = snapshot.rstrip().split()
                    name = name.split('@')[1]
                    snapshots.append({"Name" : name, "CreationTime" : int(ctime) })
            except subprocess.CalledProcessError:
                # We'll assume this is because there are no snapshots
                pass
            return snapshots
        
class ZFSBackupCount(ZFSBackup):
    def __init__(self, source, recursive=False):
        super(ZFSBackupCount, self).__init__(source, "", recursive)
        self._count = 0
        
    def __repr__(self):
        return "{}(source={}, recursive={})".format(self.__class__.__name__,
                                                    self.source,
                                                    self.recursive)
    def validate(self):
        return
    
    def backup_handler(self, stream, **kwargs):
        count = 0
        mByte = 1024 * 1024
        fobj = self._filter_backup(stream)
        while True:
            b = fobj.read(mByte)
            if b:
                count += len(b)
            else:
                break
        self._count = count

    @property
    def target_snapshots(self):
        return []
    @property
    def count(self):
        return self._count
    
def main():
    global debug
    import argparse

    def to_bool(s):
        if s.lower() in ("yes", "1", "true", "t", "y"):
            return True
        return False

    parser = argparse.ArgumentParser(description='ZFS snapshot replictor')
    parser.register('type', 'bool', to_bool)
    
    parser.add_argument("--debug", dest='debug',
                        action='store_true', default=False,
                        help='Turn on debugging')
    parser.add_argument("--verbose", dest='verbose', action='store_true',
                        default=False, help='Be verbose')
    parser.add_argument('--recursive', '-R', dest='recursive',
                        type=bool,
                        default=False,
                        help='Recursively replicate')
    parser.add_argument('--snapshot', '-S', dest='snapshot_name',
                        default=None,
                        help='Snapshot to replicate')

    parser.add_argument("--compressed", "-C", dest='compressed',
                        action='store_true', default=False,
                        help='Compress snapshots')
    
    parser.add_argument('--pigz', action='store_true',
                        dest='use_pigz', default=False,
                        help='Use pigz to compress')
    
    subparsers = parser.add_subparsers(help='sub-command help', dest='subcommand')

    # We have a sub parser for each type of replication
    # Currently just ZFS and Counter
    zfs_parser = subparsers.add_parser('zfs',
                                       help='Replicate to local ZFS dataset')
    zfs_parser.add_argument('--dest', '-D', dest='destination',
                            required=True,
                            help='Pool/dataset target for replication')

    counter_parser = subparsers.add_parser('counter',
                                           help='Count replication bytes')

    # ssh parser has a lot more options
    ssh_parser = subparsers.add_parser("ssh",
                                       help="Replicate to a remote ZFS server")
    ssh_parser.add_argument('--dest', '-D', dest='destination',
                            required=True,
                            help='Pool/dataset target for replication')
    ssh_parser.add_argument('--host', '-H', dest='remote_host',
                            required=True,
                            help='Remote hostname')
    ssh_parser.add_argument("--user", '-U', dest='remote_user',
                            help='Remote user (defaults to current user)')
    
    args = parser.parse_args()
    debug = args.debug

    if debug:
        print("args = {}".format(args), file=sys.stderr)
    
    try:
        (dataset, snapname) = args.snapshot_name.split('@')
    except ValueError:
        print("Invalid snapshot name {}".format(args.snapshot_name), file=sys.stderr)
        sys.exit(1)
        
    if args.subcommand is None:
        print("No replication type method.  Valid types are zfs, counter", file=sys.stderr)
        sys.exit(1)
    elif args.subcommand == 'counter':
        backup = ZFSBackupCount(dataset, recursive=args.recursive)
    elif args.subcommand == 'zfs':
        backup = ZFSBackup(dataset, args.destination, recursive=args.recursive)
    elif args.subcommand == 'ssh':
        backup = ZFSBackupSSH(dataset, args.destination, args.remote_host,
                              remote_user=args.remote_user,
                              recursive=args.recursive)
    else:
        print("Unknown replicator {}".format(args.subcommand), file=sys.stderr)
        sys.exit(1)

    if args.compressed:
        backup.AddFilter(ZFSBackupFilterCompressed(pigz=args.use_pigz))
            
    if args.verbose:
        print("Starting backup of {}".format(dataset))
    backup.backup(snapname=args.snapshot_name)
    if args.verbose:
        print("Done with backup");

    if isinstance(backup, ZFSBackupCount):
        print("{} bytes".format(backup.count))
        
if __name__ == "__main__":
    main()
