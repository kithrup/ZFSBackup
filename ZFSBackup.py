from __future__ import print_function
import os, sys
import json
import subprocess
import time
import tempfile
import threading
from io import BytesIO
import errno
import boto3
import botocore
import socket
        
debug = True
verbose = False

def _find_snapshot_index(name, snapshots):
    """
    Given a list of snapshots (that is, an ordered-by-creation-time
    array of dictionaries), return the index.  If it's not found,
    raise KeyError.
    """
    for indx, snapshot in enumerate(snapshots):
        if snapshot["Name"] == name:
            return indx
    raise KeyError(name)

def _last_common_snapshot(source, target):
    """
    Given a list of snapshots (which are dictionaries),
    return the last common snapshot (also as a dictionary,
    but a different one).  The inputs are a list, sorted
    by creation date.
    
    The return value -- if any -- will include:
    - Name:  (str) the name of the snapshot
    - CreationTime: (int) the creation time of the snapshot.
      This is taken from the source.
    Optional values:
    - incremental: (bool) Whether or not this was an incremental
      snapshot.  This is always taken from target.
    - parent: (str) If an incremental snapshot, then the previous
      snapshot used to create it.  This is always taken from target.
    - ResumeToken: (str) If the snapshot in question was interrupted,
      and can be resumed, this will be the value.  This value must be
      present and equal in both source and target, or else it will not
      be in the return value.
    """
    # We're going to turn the target list into a dictionary, first.
    target_dict = dict((el["Name"], el) for el in target)
    # Now we go through the source list, in reversed order, seeing
    # if the source snapshot is in target.
    for snap in reversed(source):
        if snap["Name"] in target_dict:
            t = target_dict[snap["Name"]]
            # Great, we found it!
            rv = {"Name" : snap["Name"], "CreationTime" : int(snap["CreationTime"]) }
            rv["incremental"] = t.get("incremental", False)
            if "parent" in t:
                rv["parent"] = t["parent"]
            if "ResumeToken" in snap and "ResumeToken" in t:
                if t["ResumeToken"] == snap["ResumeToken"]:
                    rv["ResumeToken"] = snap['ResumeToken']
            return rv
    return None
            
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

def _get_snapshot_size_estimate(ds, toname, fromname=None, recursive=False):
    """
    Get an estimate of the size of a snapshot.  If fromname is given, it's
    an incremental, and we start from that.
    """
    command = ["/sbin/zfs", "send", "-nPv"]
    if recursive:
        command.append("-R")
    if fromname:
        command.extend(["-i", "{}@{}".format(ds, fromname)])
    command.append("{}@{}".format(ds, toname))

    try:
        output = CHECK_OUTPUT(command, stderr=subprocess.STDOUT)
        output = output.decode("utf-8").split("\n")
        for line in output:
            if line.startswith("size"):
                (x, y) = line.split()
                if x == "size":
                    return int(y)
    except subprocess.CalledProcessError as e:
        if verbose:
            print("`{}` got exception {}".format(" ".join(command), str(e)), file=sys.stderr)
        raise
    return 0

def _get_snapshots(ds):
    """
    Return a list of snapshots for the given dataset.
    This only works for local ZFS pools, obviously.
    It relies on /sbin/zfs sorting, rather than sorting itself.
    """
    command = ["/sbin/zfs", "list", "-H", "-p", "-o", "name,creation,receive_resume_token",
               "-r", "-d", "1", "-t", "snapshot", "-s", "creation",
               ds]
    if debug:
        print("get_snapshots: {}".format(" ".join(command)), file=sys.stderr)
    try:
        output = CHECK_OUTPUT(command).decode('utf-8').split("\n")
    except subprocess.CalledProcessError:
        # We'll assume this is because there are no snapshots
        return []
    snapshots = []
    for snapshot in output:
        snapshot = snapshot.rstrip()
        if not snapshot:
            continue
        if debug:
            print("Output line: {}".format(snapshot), file=sys.stderr)
        (name, ctime, resume_token) = snapshot.split("\t")
        name = name.split('@')[1]
        d = { "Name" : name, "CreationTime" : int(ctime) }
        if resume_token != "-":
            d["ResumeToken"] = resume_token
        snapshots.append(d)
    return snapshots

class ZFSBackupError(ValueError):
    pass

class ZFSBackupMissingFullBackupError(ZFSBackupError):
    def __init__(self):
        super(ZFSBackupMissingFullBackupError).__init__(self,
                                                        "No full backup available")
        
class ZFSBackupSnapshotNotFoundError(ZFSBackupError):
    def __init__(self, snapname):
        self.snapshot_name = snapname
        super(ZFSBackupSnapshotNotFoundError).__init__(self,
                                                       "Specified snapshot {} does not exist".format(snapname))

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
    
    The transformative property indicates that the filter transforms
    the data as it processes it.  Some filters don't -- the counter
    filter, for example.  This is important for some ZFSBackups subclasses,
    such as ZFSBackupSSH, which need to apply transformative filters on
    the other end as part of the backup and restore.  By default, it's
    true; subclasses can change it, and the object can alter it.
    """
    def __init__(self, name="Null Filter"):
        self.transformative = True
        self._name = name
        
    @property
    def error_output(self):
        return None
    @error_output.setter
    def error_output(self, e):
        return
    
    @property
    def name(self):
        return self._name

    @property
    def transformative(self):
        return self._transformative
    @transformative.setter
    def transformative(self, b):
        self._transformative = b

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

    def finish(self):
        """
        Any cleanup work required for the filter.
        In the base class, that's nothing.
        """
        pass
    
class ZFSBackupFilterThread(ZFSBackupFilter):
    """
    Base class for a thread-based filter.  Either it should be
    subclassed (see ZFSBackupFilterCounter below), or it should
    be called with a callable object as the "process=" parameter.
    The process method may need to check ZFSBackupFilterThread.mode
    to decide if it is backing up or restoring.
    
    Interestingly, this doesn't seem to actually work the way I'd expected:
    when writing from a thread to a popen'd pipe, the pipe will block, even
    when a thread closes the write end of the pipe.
    """
    def __init__(self, process=None, name="Thread Filter"):
        super(ZFSBackupFilterThread, self).__init__(name=name)
        self.thread = None
        self.source = None
        self.input_pipe = None
        self.output_pipe = None
        self.transformative = False
        
    @property
    def backup_command(self):
        return None

    @property
    def restore_command(self):
        return None
    
    def process(self, buf):
        # Subclasses should do any processing here
        if self._process:
            return self._process(buf)
        else:
            return buf
        
    def run(self, *args, **kwargs):
        # We use a try/finally block to ensure
        # the write-side is always closed.
        try:
            while True:
                b = self.source.read(1024*1024)
                if b:
                    temp_buf = self.process(b)
                    os.write(self.output_pipe, b)
                else:
                    break
        finally:
            try:
                os.close(self.output_pipe)
            except OSError:
                pass
                
    def _start(self, source):
        import fcntl
        
        self.source = source
        (self.input_pipe, self.output_pipe) = os.pipe()
        # We need to set F_CLOEXEC on the output_pipe, or
        # a subsequent Popen call will keep a dangling open
        # reference around.
        flags = fcntl.fcntl(self.output_pipe, fcntl.F_GETFD)
        fcntl.fcntl(self.output_pipe, fcntl.F_SETFD, flags | fcntl.FD_CLOEXEC)
        self._py_read = os.fdopen(self.input_pipe, "rb")
        self.thread = threading.Thread(target=self.run)
        self.thread.daemon = True
        self.thread.start()
        if debug:
            print("In thread start_{}, returning {}".format(self._mode, self._py_read),
                  file=sys.stderr)
        return self._py_read
    
    def start_backup(self, source):
        if self.thread:
            self.thread = None
        self._mode = "backup"
        return self._start(source)

    def start_restore(self, source):
        if self.thread:
            self.thread = None
        self._mode = "restore"
        return self._start(source)

    def finish(self):
        if self.thread:
            self.thread.join()
        return
    
class ZFSBackupFilterCounter(ZFSBackupFilterThread):
    """
    A sample thread filter.  All this does is count the
    bytes that come in to be processed.
    """
    def __init__(self, handler=None, name="ZFS Count Filter"):
        super(ZFSBackupFilterCounter, self).__init__(name=name)
        self._count = 0
        self.handler = handler

    def process(self, b):
        self._count += len(b)
        return b

    def start_backup(self, source):
        return super(ZFSBackupFilterCounter, self).start_backup(source)

    def start_restore(self, source):
        return super(ZFSBackupFilterCounter, self).start_restore(source)
    
    @property
    def handler(self):
        return self._handler
    @handler.setter
    def handler(self, h):
        self._handler = h

    @property
    def count(self):
        # This will block until the thread is done
        self.finish()
        if self.handler and iscallable(self.handler):
            self.handler(self._count)
        return self._count

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
                 name='Command-based backup filter', error=None):
        super(ZFSBackupFilterCommand, self).__init__(name=name)
        self._backup_command=backup_command
        self._restore_command=restore_command
        self.error = error
        self.proc = None
        
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
            self.error = subprocess.DEVNULL
        self.proc = POPEN(self.restore_command,
                          bufsize=1024 * 1024,
                          stdin=source,
                          stdout=subprocess.PIPE,
                          stderr=self.error)
        return self.proc.stdout
    
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
            self.error = open("/dev/null", "wb")
        if debug:
            print("start_backup: command = {}, stdin={}, stderr={}".format(" ".join(self.backup_command),
                                                                           source,
                                                                           self.error),
                  file=sys.stderr)
        self.proc = POPEN(self.backup_command,
                          bufsize=1024 * 1024,
                          stderr=self.error,
                          stdin=source,
                          stdout=subprocess.PIPE)
        if debug:
            print("In start_bauckup for command, source = {}, proc.stdout = {}".format(source,
                                                                                       self.proc.stdout),
                  file=sys.stderr)
        return self.proc.stdout

    def finish(self):
        if self.error:
            try:
                self.error.close()
            except OSError:
                pass
            self.error = None
        if self.proc:
            self.proc.wait()
    
class ZFSBackupFilterEncrypted(ZFSBackupFilterCommand):
    """
    A filter to encrypt and decrypt a stream.
    The openssl command can do a lot more than we're asking
    of it here.
    We require a password file (for now, anyway).
    """
    def __init__(self, cipher="aes-256-cbc",
                 password_file=None):
        def ValidateCipher(cipher):
            if cipher is None:
                return False
            try:
                ciphers = CHECK_OUTPUT(["/usr/bin/openssl", "list-cipher-commands"]).split()
                return cipher in ciphers
            except subprocess.CalledProcessError:
                return False
        if password_file is None:
            raise ValueError("Password file must be set for encryption filter")

        if not ValidateCipher(cipher):
            raise ValueError("Invalid cipher {}".format(cipher))
        
        self.cipher = cipher
        self.password_file = password_file
        
        backup_command = ["/usr/bin/openssl",
                          "enc", "-{}".format(cipher),
                          "-e",
                          "-salt",
                          "-pass", "file:{}".format(password_file)]
        restore_command = ["/usr/bin/openssl",
                           "enc", "-{}".format(cipher),
                           "-d",
                           "-salt",
                           "-pass", "file:{}".format(password_file)]

        super(ZFSBackupFilterEncrypted, self).__init__(backup_command=backup_command,
                                                       restore_command=restore_command,
                                                       name='{} encryption filter'.format(self.cipher))
        
class ZFSBackupFilterCompressed(ZFSBackupFilterCommand):
    """
    A sample command filter, for compressing.
    One optional parameter:  pigz.
    """
    def __init__(self, pigz=False):
        if pigz:
            self.pigz = True
            backup_command = "/usr/local/bin/pigz"
            restore_command = "/usr/local/bin/unpigz"
            name='pigz compressor filter'
        else:
            self.pigz = False
            backup_command = "/usr/bin/gzip"
            restore_command = "/usr/bin/gunzip"
            name='gzip compressor filter'
            
        super(ZFSBackupFilterCompressed, self).__init__(backup_command=[backup_command],
                                                        restore_command=[restore_command],
                                                        name=name)
        
    @property
    def name(self):
        return "pigz compress filter" if self.pigz else "gzip compress filter"
    
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
    def filters(self):
        return self._filters
    
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
        
    def _finish_filters(self):
        # Common method to wait for all filters to finish and clean up
        for f in self.filters:
            f.finish()
            
    def _filter_backup(self, source, error=sys.stderr):
        # Private method, to stitch the backup filters together.
        input = source
        for f in self.filters:
            f.error_output = error
            if debug:
                print("Starting filter {} ({})".format(f.name, f.backup_command), file=sys.stderr)
            input = f.start_backup(input)
        return input
    
    def _filter_restore(self, source, error=None):
        # Private method, to stitch the restore filters together.
        # Note that they are in reverse order.
        input = source
        for f in reversed(self.filters):
            f.error_output = error
            if debug:
                print("Starting restore filter {} ({})".format(f.name, f.restore_ommand), file=sys.stderr)
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
        
        return

    def restore_handler(self, stream, **kwargs):
        """
        Method called to read a snapshot from the target.  In the base class,
        this simply does a 'zfs send' (with appropriate options).
        Unlike the corresponding backup_handler, restore_handler has to handle
        any setup for incremental sends.  It can know to do an incremental
        backup by having "parent" in kwargs, which will be the name of the
        base snapshot.
        
        All filters are also set up here.  In the base class, that means
        no transformative filters (since there's no real point).
        """
        command = ["/sbin/zfs", "send", "-p"]
        if self.recursive:
            command.append("-R")
        if "ResumeToken" in kwargs:
            command.extend(["-t", kwargs["ResumeToken"]])
        if "parent" in kwargs:
            command.extend(["-I", kwargs["parent"]])
        command.append("{}@{}".format(self.target, kwargs["Name"]))
        if debug:
            print(" ".join(command), file=sys.stderr)
        with tempfile.TemporaryFile() as error_output:
            # ZFS->ZFS replication doesn't use filters
            fobj = stream
            with open("/dev/null", "w+") as devnull:
                POPEN(command, stdout=fobj, stderr=error_output,
                      stdin=devnull)
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
                except subprocess.CalledProcessError:
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

    def backup(self, snapname=None,
               force_full=False,
               snapshot_handler=None,
               each_snapshot=True):
        """
        Back up the source to the target.
        If snapname is given, then that will be the snapshot used for the backup,
        otherwise it will be the most recent snapshot.  If snapname is given and
        does not exist, an exception is raised.

        After that, we then find the most recent common snapshot from source
        and target (unless force_full is True, in which case that is set to None).

        If force_full is False, it will then collect a list of snapshots on the
        source from the last common snapshot to the last snapshot.

        each_snapshot indicates whether or not to iterate over each snapshot
        between the first and last one selected.

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
                raise ZFSBackupSnapshotNotFoundError(snapname)
            # We want to remove everything in source_snapshots after the given one.
            source_snapshots = self.source_snapshots[0:snap_index+1]
        else:
            source_snapshots = self.source_snapshots
            
        # This is the last snapshot we will send, and we are guaranteed
        # by this point that it exists on the source.
        last_snapshot = source_snapshots[-1]
        if debug:
            print("last_snapshot = {}".format(last_snapshot), file=sys.stderr)

        # Next step is to get the last common snapshot.
        if force_full:
            last_common_snapshot = None
        else:
            last_common_snapshot = _last_common_snapshot(source_snapshots,
                                                         self.target_snapshots)
        if debug:
            print("ZFSBackup: last_snapshot = {}, last_common_snapshot = {}".format(last_snapshot,
                                                                                    last_common_snapshot),
                  file=sys.stderr)
        snapshot_list = source_snapshots
        if last_common_snapshot is None:
            # If we have no snapshots in common, then we do all of the snapshots
            pass
        elif last_common_snapshot["Name"] == last_snapshot["Name"]:
            # No snapshots to do, we're all done.
            if debug:
                print("No snapshots to send", file=sys.stderr)
            return
        else:
            # We have a snapshot in common in source and target,
            # and we want to get a list of snapshots from last_common_snapshot
            # to last_snapshot from snapshot_list
            # To do this, we're going to go through snapshot_list, looking
            # for the index of both last_common_snapshot and last_snapshot.
            lcs_index = None
            last_index = None
            for indx, snap in enumerate(snapshot_list):
                if snap['Name'] == last_snapshot['Name']:
                    last_index = indx
                    break
                if snap['Name'] == last_common_snapshot['Name']:
                    lcs_index = indx
            # Now we're going to do a bit of sanity checking:
            if last_index < lcs_index or lcs_index is None:
                # This seems a weird case -- the snapshot we've been
                # told to do is before the last common one.
                raise ZFSBackupError("Last snapshot in source ({}) is before last common snapshot ({})".format(last_snapshot['Name'], last_common_snapshot['Name']))
            snapshot_list = snapshot_list[lcs_index:last_index+1]

        if debug:
            print("Last common snapshot = {}".format(last_common_snapshot),
                  file=sys.stderr)
            print("\tDoing snapshots {}".format(" ".join([x["Name"] for x in snapshot_list])),
                  file=sys.stderr)

        if not each_snapshot:
            if last_common_snapshot:
                snapshot_list = (snapshot_list[0], snapshot_list[-1])
            else:
                snapshot_list = [snapshot_list[-1]]

        # At this point, snapshot_list either starts with the
        # last common snapshot, or there were no common snapshots.
        for snapshot in snapshot_list:
            resume = None
            if last_common_snapshot and snapshot["Name"] == last_common_snapshot["Name"]:
                # If we're resuming a send, we want to continue
                resume = last_common_snapshot.get("ResumeToken", None)
                if not resume:
                    # We want to skip the last common snapshot,
                    # so we can use it as the base of an incremental send
                    # in the next pass
                    continue

            command = ["/sbin/zfs", "send"]
            if self.recursive:
                command.append("-R")
            backup_dict = { "Name": snapshot["Name"] }
            backup_dict["Recursive"] = self.recursive
            try:
                backup_dict["SizeEstimate"] = _get_snapshot_size_estimate(self.source,
                                                                          snapshot["Name"],
                                                                          fromname=last_common_snapshot["Name"] if last_common_snapshot else None,
                                                                          recursive=self.recursive)
            except:
                if verbose:
                    print("Unable to get size estimate for snapshot", file=sys.stderr)
                    
            if resume:
                command.extend(["-C", resume])
                backup_dict["ResumeToken"] = resume
                
            if last_common_snapshot:
                command.extend(["-i" if each_snapshot else "-I", "{}".format(last_common_snapshot["Name"])])
                backup_dict["incremental"] = True
                backup_dict["parent"] = last_common_snapshot["Name"]
            else:
                backup_dict["incremental"] = False
            backup_dict["CreationTime"] = snapshot["CreationTime"]
            if debug:
                print("backup_dict = {}".format(backup_dict), file=sys.stderr)
                
            command.append("{}@{}".format(self.source, snapshot["Name"]))
            if debug:
                print(" ".join(command), file=sys.stderr)
            with tempfile.TemporaryFile(mode="a+") as error_output:
                with open("/dev/null", "w+") as devnull:
                    mByte = 1024 * 1024
                    send_proc = POPEN(command,
                                      bufsize=mByte,
                                      stdin=devnull,
                                      stderr=error_output,
                                      stdout=subprocess.PIPE)
                    if debug:
                        print("backup_dict = {}".format(backup_dict), file=sys.stderr)
                    if callable(snapshot_handler):
                        snapshot_handler(stage="start", **backup_dict)
                    try:
                        self.backup_handler(send_proc.stdout, **backup_dict)
                    except ZFSBackupError:
                        send_proc.wait()
                        if send_proc.returncode:
                            # We'll ignore any errors generated by the filters
                            error_output.seek(0)
                            raise ZFSBackupError(error_output.read().rstrip())
                        else:
                            raise
                    if callable(snapshot_handler):
                        snapshot_handler(stage="complete", **backup_dict)
                self._finish_filters()
            # Set the last_common_snapshot to make the next iteration an incremental
            last_common_snapshot = snapshot

        return

    def restore(self, snapname=None,
                force_full=False,
                snapshot_handler=None,
                to=None):
        """
        Perform a restore.  This is essentially the inverse of backup --
        the target is the source of data, that are sent to 'zfs recv' (with
        appropriate flags).

        If snapname is given, then the restore will be done to that
        snapshot; if force_full is False, the restore will try to find
        the most recent snapshot in common before snapname, and
        attempt an incremental restore.  Therefore the most common case
        for a restore to be done is a full restore to an empty pool/dataset,
        which may be done at once, or by restoring a series of incrementals.

        If there is no previous snapshot in common, _or_ force_full is True,
        then it will need to find the most recent full backup.  In the case
        of the base class, every snapshot is potentially a full backup, so
        it can start with snapname.  In the case of ZFSBackupDirectory,
        however, it will need to search backwards for a full backup.  If there
        are no full backups, then it will raise an exception.

        If snapname is present in both targt and source, then there will
        be no work done.  (This would be more suitable for a rollback, after
        all.)

        Any filters applied to the backup should be applied to the restore;
        subclasses that keep track of that information (ZFSBackupDirectory and
        ZFSBackupS3 at this point) will use their own knowledge of the filters
        used at backup to apply them in the correct order.  With ZFSBackup and
        ZFSBackupSSH, that's not necessary, since any data transformations are
        either ignored or undone as part of the backup process, but compression
        filters (as an example) may still be helpful to improve overall performance.
        """
        if snapname is None:
            # Get the last snapshot available on the target
            snapname = self.target_snapshots[-1]["Name"]
        try:
            snapshot_index = _find_snapshot_index(snapname, self.target_snapshots)
        except KeyError:
            raise ZFSBackupSnapshotNotFoundError(snapname)
        
        # If the snapshot is already in source, then there's nothing to do
        try:
            _find_snapshot_index(snapname, self.source_snapshots)
            return
        except KeyError:
            pass

        # We want to make sure we include the desired snapshot name.
        snapshot_list = self.target_snapshots[:snapshot_index+1]

        # Now let's look for the last common snapshot.
        # Because of the test above, we know that snapname is not in source.
        if force_full is False:
            last_common_snapshot = _last_common_snapshot(self.source_snapshots,
                                                         snapshot_list)
        else:
            last_common_snapshot = None

        # If last_common_snapshot is set, then we need a list of
        # snapshots on the target between last_common_snapshot and
        # snapname; if last_common_snapshot is None, then we
        # need a list of snapshots on the target starting with the
        # most recent full snapshot.  This is subclass-specific.
        if last_common_snapshot:
            start_index = _find_snapshot_index(last_common_snapshot["Name"],
                                               snapshot_list)
        else:
            start_index = self._most_recent_full_backup_index(snapshot_list)

        if debug:
            print("Last common snapshot = {}".format(last_common_snapshot), file=sys.stderr)
            print("start_index = {}, snapshot_list = {}".format(start_index, snapshot_list), file=sys.stderr)
            
        # This is now a list of snapshots to restore
        restore_snaps = snapshot_list[start_index:]

        if debug:
            print("Restoring snapshots {}".format(restore_snaps), file=sys.stderr)
            
        for snap in restore_snaps:
            # Do I need any other options?  Possibliy if doing
            # an interrupted restore.
            resume = None
            if last_common_snapshot and snap["Name"] == last_common_snapshot["Name"]:
                # XXX: This isn't right, I think:  we can have a resume token
                # for a full send.
                # If we're resuming we want to be able to continue
                resume = last_common_snapshot.get("ResumeToken", None)
                if not resume:
                    # We want to skip the last common snapshot, so we can use it
                    # as the basis of an incremental send.
                    continue
                
            command = ["/sbin/zfs", "receive", "-d", "-F"]
            # Copy so we can add some elements to it
            restore_dict = snap.copy()

            if last_common_snapshot:
                restore_dict["parent"] = last_common_snapshot["Name"]
            if resume:
                restore_dict["ResumeToken"] = resume
                command.extend(["-t", resume])
            elif "ResumeToken" in restore_dict:
                restore_dict.pop("ResumeToken")

            command.append(self.source)

            if debug:
                print(" ".join(command), file=sys.stderr)
            with tempfile.TemporaryFile(mode="a+") as error_output:
                with open("/dev/null", "w+") as devnull:
                    mByte = 1024 * 1024
                    if callable(snapshot_handler):
                        snapshot_handler(state="start", **restore_dict)
                    recv_proc = POPEN(command,
                                      bufsize=mByte,
                                      stdin=subprocess.PIPE,
                                      stderr=error_output,
                                      stdout=devnull)
                    
                    try:
                        self.restore_handler(recv_proc.stdin, **restore_dict)
                    except ZFSBackupError:
                        recv_proc.wait()
                        if recv_proc.returncode:
                            # We end up ignoring any errors generated by the filters
                            error_output.seek(0)
                            raise ZFSBackupError("Restore failed: {}".format(error_output.read().rstrip()))
                        else:
                            raise
                    if callable(snapshot_handler):
                        snapshot_handler(state="complete", **restore_dict)
                self._finish_filters()
            last_common_snapshot = snap
        return
    
    def _most_recent_full_backup_index(self, snapshots):
        """
        Given a list of snapshots, find the most recent full backup.
        If no full backup is given, then it raises an exception.
        """
        # For the base class, this is always simply the last snapshot
        if snapshots:
            return len(snapshots) - 1
        else:
            raise ZFSBackupMissingFullBackupError()
    
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
        command = ["/sbin/zfs", "list", "-H", "-p", "-o", "name,creation,receive_resume_token",
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
            (name, ctime, resume_token) = snapshot.rstrip().split()
            d = {"Name" : name, "CreationTime" : int(ctime) }
            if resume_token != "-":
                d["ResumeToken"] = resume_token
            snapshots.append(d)
            
        return snapshots

    def Check(self, **kwargs):
        """
        A method to do a verification that the backup is okay.
        In the base class, we don't do anything.
        """
        pass
    
class ZFSBackupDirectory(ZFSBackup):
    """
    A variant of ZFSBackup that backs up to files, rather than replication.
    The layout used is:
     target/
      prefix/
       map.json
       chunks/
        data files

    prefix will default to the hostname if none is given.
    target is the root pathname -- note that this doesn't need to be
    a ZFS filesystem.

    The map file maps from dataset to snapshots.
    Since some filesystems (I'm looking at you, msdos) have a
    limit of 4gb, we'll keep chunks limited to 2gb.

    Each dataset has a chronologically-ordered array of
    snapshots.

    A snapshot entry in the map contains the name, the
    creation time, whether it is recursive, and, if it
    is an incremental snapshot, what the previous one was.
    It also contains the names of the chunks, and any transformative
    filter commands (in order to restore it).
    
    """
    def __init__(self, source, target, prefix=None, recursive=False):
        self._prefix = prefix or socket.gethostname()
        self._mapfile = None
        self._chunk_dirname = "chunks"
        super(ZFSBackupDirectory, self).__init__(source, target, recursive)


    def __repr__(self):
        return "{}({}, {}, prefix={}, recursive={})".format(self.__class__.__name__,
                                                            self.source, self.target,
                                                            self.prefix, self.recursive)
    
    def validate(self):
        """
        Ensure that the destination exists.  Since this is just
        using files, all we need is os.path.exists
        """
        if not os.path.exists(self.target):
            raise ZFSBackupError("Target {} does not exist".format(self.target))
        return

    @property
    def mapfile(self):
        """
        Return the mapfile.  If it isn't loaded, we load it now.
        """
        if self._mapfile is None:
            mapfile_path = os.path.join(self.target, self.prefix, "map.json")
            try:
                with open(mapfile_path, "r") as mapfile:
                    self._mapfile = json.load(mapfile)
            except:
                # I know, blanket catch, shouldn't do that
                self._mapfile = {}

        return self._mapfile
    @mapfile.setter
    def mapfile(self, d):
        if debug:
            print("Setting mapfile to {}".format(d), file=sys.stderr)
        if not self._mapfile or self._mapfile != d:
            self._mapfile = d
            self._save_mapfile()
            
    def _save_mapfile(self):
        """
        Save the map file.
        """
        if self._mapfile:
            mapfile_path = os.path.join(self.target, self.prefix, "map.json")
            if debug:
                print("Saving map file to {}".format(mapfile_path), file=sys.stderr)
            with open(mapfile_path, "w") as mapfile:
                json.dump(self._mapfile, mapfile,
                          sort_keys=True,
                          indent=4, separators=(',', ': '))

    @property
    def target_snapshots(self):
        """
        The snapshots are in the mapfile.
        First key we care about is the source dataset.
        """
        m = self.mapfile
        if debug:
            print("mapfile = {}".format(m), file=sys.stderr)
        if self.source in m:
            return m[self.source]["snapshots"]
        else:
            return []

    def _write_chunks(self, stream):
        chunks = []
        mByte = 1024 * 1024
        gByte = 1024 * mByte
        done = False

        base_path = os.path.join(self.target, self.prefix)
        chunk_dir = os.path.join(base_path, self._chunk_dirname)
        for d in (base_path, chunk_dir):
            try:
                os.makedirs(d)
            except OSError as e:
                if e.errno != errno.EEXIST:
                    raise

        while not done:
            with tempfile.NamedTemporaryFile(dir=chunk_dir, delete=False) as chunk:
                chunks.append(os.path.join(self.prefix,
                                           self._chunk_dirname,
                                           os.path.basename(chunk.name)))
                total = 0
                while total < 2*gByte:
                    buf = stream.read(mByte)
                    if not buf:
                        done = True
                        break
                    chunk.write(buf)
                    total += len(buf)
                if debug:
                    print("Finished writing chunk file {}".format(chunk.name), file=sys.stderr)
        return chunks
    
    def backup_handler(self, stream, **kwargs):
        # Write the backup to the target.  In our case, we're
        # doing a couple of things:
        # First, we need to make sure the full target directory
        # exists -- create it if necessary.

        # Sanity check: unlike the base class, we need to
        # know the name of the snapshot, and whether it's incremental.
        # If it is, we also need to know the previous one

        snapshot_name = kwargs.get("Name", None)
        incremental = kwargs.get("incremental", None)
        parent = kwargs.get("parent", None)

        if snapshot_name is None:
            raise ZFSBackupError("Missing name of snapshot")

        if incremental is None:
            raise ZFSBackupError("Missing incremental information about snapshot")

        # Next sanity check: if this snapshot is already in the map, abort
        source_map = self.mapfile.get(self.source, {})
        current_snapshots = source_map.get("snapshots", [])
        
        for x in current_snapshots:
            if x["Name"] == snapshot_name:
                raise ZFSBackupError("Snapshot {} is already present in target".format(snapshot_name))
        
        filters = []
        for f in reversed(self.filters):
            if f.transformative and f.restore_command:
                filters.append(f.restore_command)
                
        # Now we need to start writing chunks, keeping track of their names.
        with tempfile.TemporaryFile() as error_output:
            fobj = self._filter_backup(stream, error=error_output)
            chunks = self._write_chunks(fobj)
            if not chunks:
                error_output.seek(0)
                raise ZFSBackupError(error_output.read())

        # Now we need to update the map to have the chunks.
        snapshot_dict = {
            "Name" : snapshot_name,
            "CreationTime" : kwargs.get("CreationTime", int(time.time())),
            "incremental": incremental,
            "chunks" : chunks
        }
        if incremental:
            snapshot_dict["parent"] = parent
        if filters:
            snapshot_dict["filters"] = filters
        for key in kwargs.keys():
            if key in ("Name", "CreationTime", "incremental", "chunks",
                       "parent", "filters"):
                continue
            snapshot_dict[key] = kwargs.get(key)
            
        current_snapshots.append(snapshot_dict)
        source_map["snapshots"] = current_snapshots
        self.mapfile[self.source] = source_map
        self._save_mapfile()
        
                    
    @property
    def prefix(self):
        return self._prefix
    

    def _get_all_chunks(self):
        """
        Returns a set of all the chunks in self.target/self.prefix/self._chunk_dirname
        """
        rv = set()
        chunk_dir = os.path.join(self.prefix, self._chunk_dirname)
        for entry in os.listdir(os.path.join(self.target, chunk_dir)):
            if os.path.isdir(os.path.join(self.target, chunk_dir, entry)):
                # This shouldn't be the case
                continue
            rv.add(os.path.join(chunk_dir, entry))
        return rv
    
    def Check(self, **kwargs):
        """
        Method to ensure that the backup is sane.
        In this case, it means checking that every chunk
        in the directory is accounted for.  We also check
        to see if every snapshot has all of the chunks it
        lists, and ensure that every incrememental snapshot
        has its parent, all the way to a non-incremental.

        If there are any problems, we return a list of them.

        If cleanup=True in kwargs, we'll clean up the problems
        (still returning the list). (Not yet implemented.)

        N.B. Due to the nature of this method and class, it
        will remove *all* untracked chunks; however, it will only
        do a consistency check for the specified dataset, unless
        check_all=True in kwargs.
        """
        problems = []
        
        cleanup = kwargs.get("cleanup", False)
        check_all = kwargs.get("check_all", False)

        # First step is to get the backups from the mapfile.
        backups = self.mapfile.keys()

        # Next we want to get a list of all the chunks.
        # These will be relative to the target directory,
        # so we'll turn them into ${prefix}/${chunkdir}/${chunkname}
        # Since we don't care about order, but do care about lookup,
        # we'll put them into a set.
        directory_chunks = self._get_all_chunks()
            
        # Let's now ensure every chunk is accounted for
        # We put them all into another set
        mapfile_chunks = set()
        for backup in self.mapfile.itervalues():
            for snapshot in backup['snapshots']:
                for chunk in snapshot['chunks']:
                    mapfile_chunks.add(chunk)

        # Let's see if there are any extraneous files
        extra_chunks = directory_chunks - mapfile_chunks
        # And voila, we have a list of chunks that have gone orphaned
        for chunk in extra_chunks:
            problems.append(("delete_chunk", chunk))

        # Next pass, let's ensure that the backups have all of
        # their chunks.
        # If check_all is True, we'll look at all of the backups,
        # otherwise just ours.

        if not check_all:
            backups = [self.source]
        for backup in backups:
            snapshot_names = {}
            for snapshot in self.mapfile[backup]["snapshots"]:
                # The list is supposed to be in order
                name = snapshot["Name"]
                snapshot_names[name] = True
                found_all = True
                if verbose:
                    print("Checking {}@{}".format(backup, name), file=sys.stderr)
                for chunk in snapshot["chunks"]:
                    if not chunk in directory_chunks:
                        found_all = False
                        break
                if snapshot.get("incremental", False):
                    if snapshot["parent"] not in snapshot_names:
                        problems.append(("missing_parent", backup, name, snapshot["parent"]))
                if not found_all:
                    problems.append(("corrupt_snapshot", backup, name))

                
        return problems

class ZFSBackupS3(ZFSBackupDirectory):
    """
    Backup to AWS.  Optionally with transitions to glacier.
    The layout used is:
     bucket/
      prefix/
       map.json
      chunks/
        data files

    The map file maps from dataset to snapshots.
    A glacier file is limited to 40tb (and S3 to 5tb),
    so we'll actually break the snapshots into 4gbyte
    chunks.

    We control a lifecycle rule for bucket, which we
    will name "${prefix} ZFS Backup Rule"; if glacier
    is enabled, we add that rule, and set glacier migration
    for "chunks/" for 0 days; if it is not
    enabled, then we set the rule to be disabled.  (But
    we always have the rule there.)

    Each dataset has a chronologically-ordered array of
    snapshots.

    A snapshot entry in the map contains the name, the
    creation time, whether it is recursive, and, if it
    is an incremental snapshot, what the previous one was.
    It also contains the names of the chunks.

    So it looks something like:

    "tank" : [
	"auto-daily-2017-01-01:00:00" : {
	    "CreationTime" : 12345678,
            "Size"         : 1024000,
            "Recursive"    : True,
            "Incremental"  : null,
	    "Chunks"       : [
		"chunks/${random}",
		"chunks/${random}"
	    ]
         },
	"auto-daily-2017-01-02:00:00" : {
	...
	}
    ]

    Each dataset being backed up has an entry in the map file.
    """
    
    def __init__(self, source,
                 bucket, s3_key, s3_secret,
                 recursive=False, server=None,
                 prefix=None, region=None, glacier=True):
        """
        Backing up to S3 requires a key, secret, and bucket.
        If prefix is none, it will use the current hostname.
        (As a result, prefix must be unique within the bucket.)

        If the bucket doesn't exist, it gets created; if
        glacier is True, then it will set up a transition rule.

        Note that bucket names need to be globally unique.
        """
        self._map = None
        self._glacier = glacier

        self._s3 = boto3.client('s3', aws_access_key_id=s3_key,
                                aws_secret_access_key=s3_secret,
                                endpoint_url=server,
                                region_name=region)

        # Note that this may not exist.
        self.bucket = bucket.lower()
        self._prefix = prefix or socket.gethostname()
        # We'll over-load prefix here
        super(ZFSBackupS3, self).__init__(source, "",
                                          prefix=prefix,
                                          recursive=recursive)
        self._setup_bucket()
        
    def validate(self):
        if debug:
            print("* * * HERE I AM NOW * * *\n\n", file=sys.stderr)
        if debug:
            print("\nRunning setup_bucket\n")
        return
    
    def __repr__(self):
        return "{}({}, {}, <ID>, <SECRET>, recursive={}, server={}, prefix={}, region={}, glacier={}".format(
            self.__class__.__name__, self.source, self.bucket, self.recursive, self.server,
            self.prefix, self.region, self.glacier)

    def _setup_bucket(self):
        """
        Create a bucket, if necessary.  Also, set up the lifecycle rule
        depending on whether or not we're using glacier.
        """
        if debug:
            print("Trying to setup bucket {}".format(self.bucket), file=sys.stderr)
            
        try:
            self.s3.head_bucket(Bucket=self.bucket)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]['Code'] == '404':
                # Need to create the bucket
                if debug:
                    print("Creating bucket {}".format(self.bucket))
                result = self.s3.create_bucket(Bucket=self.bucket)
                if debug:
                    print("When creating bucket {}, response is {}".format(self.bucket, result),
                          file=sys.stderr)
            else:
                raise
        # Great, now we have a bucket for sure, or have exceptioned out.
        # Now we want to get the lifecycle rules.
        try:
            lifecycle = self.s3.get_bucket_lifecycle_configuration(Bucket=self.bucket)
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]['Code'] == 'NoSuchLifecycleConfiguration':
                lifecycle = {}
            elif e.response['Error']['Code'] == "NotImplemented":
                lifecycle = None
            else:
                raise
        if lifecycle is not None:
            try:
                rules = lifecycle["Rules"]
            except KeyError:
                rules = []

            rule_id = "{} ZFS Backup Glacier Transition Rule".format(self.prefix)
            rule_indx = None
            changed = False
            if rules:
                if debug:
                    print("Trying to add/set lifecycle rule", file=sys.stderr)
                for indx, rule in enumerate(rules):
                    if rule["ID"] == rule_id:
                        rule_indx = indx
                        break
                if debug:
                    print("rule_indx = {}, appropriate rule = {}".format(rule_indx,
                                                                         rules[rule_indx] if rule_indx is not None else "<no rules>"), file=sys.stderr)
            if rule_indx is None:
                # We need to add it
                new_rule = {
                    "ID" : rule_id,
                    "Prefix" : "{}/".format(self._chunk_dirname),
                    "Status" : "Enabled",
                    "Transitions" : [
                        {
                            "Days" : 0,
                            "StorageClass" : "GLACIER"
                        },
                    ],
# Does this prevent transitions from working?
#                    'AbortIncompleteMultipartUpload' : {
#                        'DaysAfterInitiation' : 7,
#                    },
                }
                rule_indx = len(rules)
                rules.append(new_rule)
                changed = True
                if debug:
                    print("rule_indx = {}, rules = {}".format(rule_indx, rules), file=sys.stderr)
            else:
                if (self.glacier == ( rules[rule_indx]["Status"] == "Enabled")):
                    changed = False
            if debug:
                    print("rule_indx = {}, changed = {}, rules = {}, now let's set it to enabled".format(rule_indx, changed, rules), file=sys.stderr)
            rules[rule_indx]["Status"] = "Enabled" if self.glacier else "Disabled"
            if changed:
                if debug:
                    print("rules = {}".format(rules), file=sys.stderr)
                self.s3.put_bucket_lifecycle_configuration(Bucket=self.bucket,
                                                           LifecycleConfiguration={ 'Rules' : rules }
                                                           )
        return
        
    @property
    def glacier(self):
        return self._glacier
    @property
    def prefix(self):
        return self._prefix
    @property
    def s3(self):
        return self._s3

    @property
    def bucket(self):
        return self._bucket
    @bucket.setter
    def bucket(self, b):
        self._bucket = b
        
    def _key_exists(self, keyname):
        try:
            self.s3.head_object(Bucket=self.bucket,
                                Key=keyname)
            return True
        except botocore.exceptions.ClientError:
            return False
                
    @property
    def mapfile(self):
        """
        Load the map file from the bucket.  We cache it so we
        don't keep reloading it.
        """
        if self._mapfile is None:
            # Check to see if the map file exists in the bucket
            map_key = "{}/map.json".format(self.prefix)
            if self._key_exists(map_key):
                map_file = BytesIO()
                self.s3.download_fileobj(Bucket=self.bucket,
                                         Key=map_key,
                                         Fileobj=map_file)
                map_file.seek(0)
                self._mapfile = json.loads(map_file.getvalue().decode('utf-8'))
            else:
                if debug:
                    print("mapfile {} does not exist in bucket".format(map_key), file=sys.stderr)
                self._mapfile = {}
        return self._mapfile
    @mapfile.setter
    def mapfile(self, mf):
        self._mapfile = mf

    def _save_mapfile(self):
        if self._mapfile:
            map_key = "{}/map.json".format(self.prefix)
            buffer = json.dumps(self._mapfile).encode('utf-8')
            map_file = BytesIO(buffer)
            map_file.seek(0)
            self.s3.upload_fileobj(Bucket=self.bucket,
                                   Key=map_key,
                                   Fileobj=map_file)
            
    def _write_chunks(self, stream):
        import binascii
        
        chunks = []
        mByte = 1024 * 1024
        gByte = 1024 * mByte
        done = False

        chunk_dir = os.path.join(self._chunk_dirname, self.prefix)

        while not done:
            while True:
                chunk_key = binascii.b2a_hex(os.urandom(32)).decode('utf-8')
                chunk_key = os.path.join(chunk_dir, chunk_key)
                if not self._key_exists(chunk_key):
                    break
            total = 0
            uploader = self.s3.create_multipart_upload(Bucket=self.bucket,
                                                       ACL='private',
                                                       Key=chunk_key)
            upload_id = uploader['UploadId']
            parts = []
            try:
                while total < 4*gByte:
                    part_num = len(parts) + 1
                    buf = stream.read(10*mByte)
                    if not buf:
                        if debug:
                            print("Breaking out of loop after {} bytes".format(total), file=sys.stderr)
                        done = True
                        break
                    # We need to upload this 10Mbyte part somehow
                    response = self.s3.upload_part(Bucket=self.bucket,
                                                   Key=chunk_key,
                                                   Body=buf,
                                                   PartNumber=part_num,
                                                   UploadId=upload_id)
                    if debug:
                        print("response = {}".format(response), file=sys.stderr)
                    parts.append({ "ETag" : response["ETag"], "PartNumber" : part_num })
                    total += len(buf)
                if parts:
                    if debug:
                        print("After {} parts, completing upload".format(len(parts)), file=sys.stderr)
                    self.s3.complete_multipart_upload(Bucket=self.bucket,
                                                      Key=chunk_key,
                                                      UploadId=upload_id,
                                                      MultipartUpload={ "Parts" : parts })
            except:
                # This blanket exception catch is intentional
                if verbose:
                    print("Aborting multipart upload after {} parts".format(len(parts)), file=sys.stderr)
                self.s3.abort_multipart_upload(Bucket=self.bucket,
                                               Key=chunk_key,
                                               UploadId=upload_id)
                raise    
            chunks.append(chunk_key)
            if debug:
                print("Wrote {} bytes to chunk {}".format(total, chunk_key), file=sys.stderr)
            total = 0
        if debug:
            print("Wrote out {} chunks".format(len(chunks)), file=sys.stderr)
        return chunks
    
    def validate(self):
        """
        We don't do a lot of validation, since s3 costs per usage.
        We'll lazily check the bucket, and create it if necessary.
        """
        return
    
    def AvailableRegions():
        """
        List the available regons.
        """
        return boto3.session.Session().get_available_regions('s3')
    
    def _get_all_chunks(self):
        """
        Returns a set of all the chunks -- keys, in AWS parlance --
        that begin with self.bucket/self._chunk_dir/self.prefix/
        """
        rv = set()
        last_string = ''
        while True:
            response = self.s3.list_objects_v2(Bucket=self.bucket,
                                               Prefix=os.path.join(self._chunk_dirname, self.prefix),
                                               StartAfter=last_string)
            for key in [x.get("Key") for x in response.get("Contents")]:
                last_string = key
                rv.add(key)
            if response.get("IsTruncated") == False:
                break
        return rv
    
    def Check(self, **kwargs):
        """
        Check an S3 backup destination.
        This uses the base class, and then checks for multipart uploads.
        """
        from datetime import datetime, timedelta
        problems = super(ZFSBackupS3, self).Check(**kwargs)

        # Now we check for multipart uploads in our bucket
        try:
            uploads = self.s3.list_multipart_uploads(Bucket=self.bucket)
        except botocore.exceptions.ClientError:
            return problems

        for upload in uploads.get("Uploads", []):
            upload_id = upload["UploadId"]
            upload_key = upload["Key"]
            # Is this correct?
            initiated = upload["Initiated"]
            now = datetime.now()
            delta = now - intitiated
            if delta.days > 2:
                problems.append(("stale_multpart_upload", self.bucket, upload_key, upload_id))
                
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
        for arg in args:
            command.append('"{}"'.format(arg))
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
        return POPEN(command[0], *command[1:], **kwargs)
    
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

    def restore_handler(self, stream, **kwargs):
        """
        Restore from a remote ZFS dataset (via ssh).
        """
        command = ["/sbin/zfs", "send", "-p"]
        if self.recursive:
            command.append("-R")
        if "ResumeToken" in kwargs:
            command.extend(["-t", kwargs["ResumeToken"]])
        if parent in kwargs:
            command.extend(["-I", kwargs["parent"]])
        command.append("{}@{}".format(self.target, kwargs["Name"]))
        # If we have any transformative filters, we need to create them in order.
        # Note that, as counterintuitive as it may seem, we use the backup_command for
        # each filter on the remote side.
        for filter in self.filters:
            if filter.transformative and filter.backup_command:
                commannd = command + ["|"] + filter.backup_command
                
        command = self._build_command(*command)
        if debug:
            print("Remote restore command: " + " ".join(command), file=sys.stderr)

        with tempfile.TemporaryFile() as error_output:
            fobj = self._filter_restore(stream, error=error_output)
            try:
                CHECK_CALL(command, stdout=fobj, stderr=error_output)
            except subprocess.CalledProcessError:
                error_output.seek(0)
                raise ZFSBackupError(error_output.read().rstrip())

        return
                
    def backup_handler(self, stream, **kwargs):
        """
        Implement the replication.
        """

        # First, we create the intervening dataset paths. See the base class' method.
        full_path = self.target
        with open("/dev/null", "w+") as devnull:
            for d in self.source.split("/")[1:]:
                full_path = os.path.join(full_path, d)
                command = self._build_command("/sbin/zfs", "create", "-o", "readonly=on", full_path)
                try:
                    CALL(command, stdout=devnull, stderr=devnull, stdin=devnull)
                except subprocess.CalledProcessError:
                    pass
                
        # If we have any transformative filters, we need to create them in reverse order.
        command = ["/sbin/zfs", "receive", "-d", "-F", self.target]
        for filter in reversed(self.filters):
            if filter.transformative and filter.restore_command:
                command = filter.restore_command + ["|"] + command
                
        command = self._build_command(*command)
        if debug:
            print("backup command = {}".format(command), file=sys.stderr)
        with tempfile.TemporaryFile() as error_output:
            fobj = self._filter_backup(stream, error=error_output)
            try:
                CHECK_CALL(command, stdin=fobj, stderr=error_output)
            except subprocess.CalledProcessError:
                error_output.seek(0)
                raise ZFSBackupError(error_output.read())
            fobj.close()
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
        fobj = self._filter_backup(stream)
        mByte = 1024 * 1024
        while True:
            b = fobj.read(mByte)
            if b:
                self._count += len(b)
            else:
                break

    @property
    def target_snapshots(self):
        return []
    @property
    def count(self):
        return self._count
    
def parse_operation(args):
    """
    Determine which operation, and what options for it.
    Default is to just parse ["backup"]
    """
    import argparse
    
    def to_bool(s):
        if s.lower() in ("yes", "1", "true", "t", "y"):
            return True
        return False

    parser = argparse.ArgumentParser(description="Operation and options")
    parser.register('type', 'bool', to_bool)

    if not args:
        args = ["backup"]

    ops = parser.add_subparsers(help='sub-operation help', dest='command')

    # The current valid operations are backup, restore, list, verify, and delete
    # Although only backup and restore are currently implemented
    backup_operation = ops.add_parser("backup", help="Backup command")

    restore_operation = ops.add_parser("restore", help='Restore command')

    verify_operation = ops.add_parser("verify", help='Verify command')
    verify_operation.add_argument("--all", action='store_true', dest='check_all',
                                  help='Check every backup for consistency',
                                  default=False)
    
    delete_operation = ops.add_parser('delete', help='Delete command')

    list_operation = ops.add_parser("list", help='List command')
    
    rv = parser.parse_args(args)
    return rv

def parse_arguments(args=None):
    global debug, verbose
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
                        action='store_true',
                        default=False,
                        help='Recursively replicate')
     
    parser.add_argument("--snapshot", "-S", "--dataset", "--pool",
                        dest='snapshot_name',
                        default=None,
                        help='Dataset/pool/snapshot to back up')
    
    parser.add_argument("--encrypted", "-E", dest='encrypted',
                        action='store_true', default=False,
                        help='Encrypt snapshots')
    parser.add_argument("--cipher", dest='cipher',
                        default='aes-256-cbc',
                        help='Encryption cipher to use')
    parser.add_argument('--password-file', dest='password_file',
                        default=None,
                        help='Password file for encryption')
    
    parser.add_argument("--compressed", "-C", dest='compressed',
                        action='store_true', default=False,
                        help='Compress snapshots')
    
    parser.add_argument('--pigz', action='store_true',
                        dest='use_pigz', default=False,
                        help='Use pigz to compress')
    
    incrementals = parser.add_mutually_exclusive_group()
    incrementals.add_argument("--iterate-incrementals", dest="iterate",
                              action='store_true', default=True)
    incrementals.add_argument("--no-iterate-incrementals", dest="iterate",
                              action='store_false')
    
    subparsers = parser.add_subparsers(help='sub-command help', dest='subcommand')

    # We have a sub parser for each type of replication
    # Currently just ZFS and Counter
    zfs_parser = subparsers.add_parser('zfs',
                                       help='Replicate to local ZFS dataset')
    zfs_parser.add_argument('--dest', '-D', dest='destination',
                            required=True,
                            help='Pool/dataset target for replication')
    zfs_parser.add_argument("rest", nargs=argparse.REMAINDER)

    counter_parser = subparsers.add_parser('counter',
                                           help='Count replication bytes')
    counter_parser.add_argument("rest", nargs=argparse.REMAINDER)

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
    ssh_parser.add_argument("rest", nargs=argparse.REMAINDER)

    # Directory parser has only two options
    directory_parser = subparsers.add_parser("directory",
                                        help='Save snapshots to a directory')
    directory_parser.add_argument("--dest", "-D", dest='destination', required=True,
                                  help='Path to store snapshots')
    directory_parser.add_argument("--prefix", "-P", dest='prefix', default=None,
                                  help='Prefix to use when saving snapshots (defaults to hostname)')
    directory_parser.add_argument("rest", nargs=argparse.REMAINDER)

    # S3 parser has many options
    s3_parser = subparsers.add_parser("s3", help="Save snapshots to an S3 server")
    s3_parser.add_argument("--bucket", dest='bucket_name', required=True,
                           help='Name of bucket in which to save data')
    s3_parser.add_argument("--prefix", dest='prefix', default=None,
                           help='Prefix (inside of bucket); defaults to host name)')
    s3_parser.add_argument("--key", "--s3-id", dest='s3_key', required=True,
                           help='S3 Access ID')
    s3_parser.add_argument("--secret", dest='s3_secret', required=True,
                           help='S3 Secret Key')
    s3_parser.add_argument('--server', dest="s3_server", default=None,
                           help='S3-compatible server')
    glacier = s3_parser.add_mutually_exclusive_group()
    glacier.add_argument("--glacier", dest='glacier', action='store_true', default=True)
    glacier.add_argument("--no-glacier", dest='glacier', action='store_false')
    
    s3_parser.add_argument('--glacer', dest='glacier', default=True,
                           type=bool, help='Use Glacier transitioning')
    s3_parser.add_argument('--region', dest='region', default=None,
                           help='S3 Region to use')
    s3_parser.add_argument("rest", nargs=argparse.REMAINDER)
    
    rv = parser.parse_args(args)

    if rv.subcommand is None:
        parser.print_help()
        sys.exit(1)
        
    return rv
    

def main():
    global debug, verbose

    args = parse_arguments()
    
    operation = parse_operation(args.rest)
        
    # Start doing some sanity checks

    # Due to the complexity of encryption, we need to handle
    # some cases that (as far as I can tell) argparse doesn't.
    if args.encrypted:
        if args.password_file is None:
            print("Password file is required when encrypting backups", file=sys.stderr)
            sys.exit(1)
        if args.subcommand == "ssh":
            print("Encrypting while using ssh replication is not possible", file=sys.stderr)
            sys.exit(1)
            
    verbose = args.verbose
    debug = args.debug
    if debug:
        verbose = True
        
    if debug:
        print("args = {}".format(args), file=sys.stderr)
    
    try:
        (dataset, snapname) = args.snapshot_name.split('@')
    except ValueError:
        dataset = args.snapshot_name
        snapname = None
        
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
    elif args.subcommand == 'directory':
        backup = ZFSBackupDirectory(dataset, args.destination, recursive=args.recursive,
                                    prefix=args.prefix)
    elif args.subcommand == 's3':
        backup = ZFSBackupS3(dataset, args.bucket_name, args.s3_key, args.s3_secret,
                             recursive=args.recursive, server=args.s3_server,
                             prefix=args.prefix, region=args.region, glacier=args.glacier)
    else:
        print("Unknown replicator {}".format(args.subcommand), file=sys.stderr)
        sys.exit(1)

    before_count = None; after_count = None
    if args.compressed:
        if verbose:
            before_count = ZFSBackupFilterCounter()
            backup.AddFilter(before_count)
        backup.AddFilter(ZFSBackupFilterCompressed(pigz=args.use_pigz))
        if verbose:
            after_count = ZFSBackupFilterCounter()
            backup.AddFilter(after_count)
            
    if args.encrypted:
        encrypted_filter = ZFSBackupFilterEncrypted(cipher=args.cipher,
                                                    password_file=args.password_file)
        backup.AddFilter(encrypted_filter)
        
    if operation.command == "backup":
        def handler(**kwargs):
            stage = kwargs.get("stage", "")
            if stage == "start":
                print("Starting backup of snapshot {}@{}".format(dataset, kwargs.get("Name")))
            elif stage == "complete":
                print("Completed backup of snapshot {}@{}".format(dataset, kwargs.get("Name")))
                
        if verbose:
            print("Starting backup of {}".format(dataset))
            
        try:
            backup.backup(snapname=snapname,
                          snapshot_handler=handler if verbose else None,
                          each_snapshot=args.iterate)
            if args.verbose:
                print("Done with backup");
        except ZFSBackupError as e:
            print("Backup failed: {}".format(e.message), file=sys.stderr)
    elif operation.command == "restore":
        def handler(**kwargs):
            stage = kwargs.get("stage", "")
            if stage == "start":
                print("Starting restore of snapshot {}@{}".format(dataset, kwargs.get("Name")))
            elif stage == "complete":
                print("Completed restore of snapshot {}@{}".format(dataset, kwargs.get("Name")))
        if verbose:
            print("Starting restore of {}".format(dataset))
        try:
            backup.restore(snapname=snapname,
                           snapshot_handler=handler if verbose else None)
            if verbose:
                print("Done with restore")
        except ZFSBackupError as e:
            print("Restore failed: {}".format(e.message), file=sys.stderr)
    elif operation.command == 'verify':
        problems = backup.Check(check_all=operation.check_all)
        if problems:
            print(problems)
        elif verbose:
            print("No problems")
    elif operation.command == "list":
        # List snapshots
        if debug:
            print("Listing snapshots", file=sys.stderr)
        for snapshot in backup.target_snapshots:
            output = "Snapshot {}@{}".format(dataset, snapshot["Name"])
            if verbose:
                ctime = time.localtime(snapshot.get("CreationTime", 0))
                output += "\n\tCreated {}".format(time.strftime("%a, %d %b %Y %H:%M:%S %z", ctime))
                if snapshot.get("incremental", False):
                    output += "\n\tincremental parent={}".format(snapshot.get("parent", "<unknown>"))
                filters = snapshot.get("filters", [])
                for filter in filters:
                    output += "\n\tFilter: {}".format(" ".join(filter))
                if "chunks" in snapshot:
                    output += "\n\tChunks:\n"
                    for chunk in snapshot["chunks"]:
                        output += "\t\t{}".format(chunk)
                for key in snapshot.keys():
                    if key in ("Name", "CreationTime", "incremental",
                               "parent", "chunks", "filters"):
                        continue
                    output += "\n\t{} = {}".format(key, snapshot[key])
            print(output)


    if operation.command in ("backup", "restore"):
        if isinstance(backup, ZFSBackupCount):
            output = "{} bytes".format(backup.count)
            print(output)
        
        if before_count and before_count.count and after_count:
            pct = (after_count.count * 100.0) / before_count.count
            output = "Compressed {} to {} bytes ({:.2f}%)".format(before_count.count,
                                                                  after_count.count,
                                                                  pct)
            print(output)
        
if __name__ == "__main__":
    main()
