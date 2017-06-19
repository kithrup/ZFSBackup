from __future__ import print_function
import os, sys
import subprocess
import time
import tempfile
import threading

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
    """
    def __init__(self, backup_command=["/bin/cat"],
                 restore_command=["/bin/cat"], error=None):
        super(ZFSBackupFilterCommand, self).__init__()
        self._backup_command=backup_command.copy()
        self._restore_command=restore_command.copy()
        self.error = error
        
    @property
    def backup_command(self):
        return self._backup_command
    @property
    def restore_command(self):
        return self._restore_command
    @property
    def error(self):
        return self._error
    @error.setter
    def error(self, where):
        if self.error is not None:
            self.error.close()
        self._error = where

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
        p = subprocess.Popen(self.restore_command,
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
        p = subprocess.Popen(self.backup_command,
                             bufsize=1024 * 1024,
                             stderr=self.error,
                             stdin=source,
                             stdout=subprocess.PIPE)
        return p.stdout
    
class ZFSBackupFilterCounter(ZFSBackupFilterThread):
    """
    A sample thread filter.  All this does is count the
    bytes that come in to be processed.
    """
    def __init__(self):
        super(ZFSBackupFilterCounter, self).__init__()
        self._count = 0
        
    def name(self):
        return "ZFS Count Filter"

    def process(self, b):
        self._count += len(b)
        return b

    @property
    def count(self):
        self._done.wait()
        return self._count

class ZFSBackup(object):
    """
    Base class for ZFS backup.
    This doesn't do anything for backup other than dump
    zfs send to stdout; for restore, it reads from stdin
    and does a zfs recv.
    """
    def __init__(self, dataset, recursive=False):
        self._ds = dataset
        self._recursive = recursive

    def snapshots(self):
        return []
    
    @property
    def dataset(self):
        return self._ds

    @property
    def recursive(self):
        return self._recursive

class ZFSReplication(object):
    """
    Base class for doing ZFS replication.
    """
    def __init__(self, target, dataset, recursive=False):
        # The only thing we need to do here is ensure the target
        # exists.  We'll call a method to validate it, so subclassing
        # works
        self._dest = target
        self._ds = dataset
        self._recursive = recursive
        self.validate()
        self._filters = None
        
    def AddFilter(self, cmd, *args):
        # Add a filter.  This is invoked during replicate,
        # and is set up as a pipeline in the order created.
        # That is: AddFilter("/bin/cat"); AddFilter("/usr/bin/sort")
        # would do the equivalent of "<input source> | cat | sort | <target>".
        x = [cmd]
        if args: x.extend(args)
        if self._filters is None:
            self._filters = []
        self._filters.append(x)

    def _filter(self, source, error=None):
        # Sets up the filters (if any), and returns the stdout of
        # the last one.  If there are none, it returns source.
        if self._filters is None:
            return source
        input = source
        mByte = 1024 * 1024
        for f in self._filters:
            p = subprocess.Popen(f, bufsize=mByte,
                                 stdin=input,
                                 stderr=error,
                                 stdout=subprocess.PIPE)
            input = p.stdout
        return input
    
    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.target)
    
    def validate(self):
        """
        Ensure the destination exists.  Derived classes will want
        to override this.
        We also need to create the datasets on target as necessary.
        This would be better with libzfs.
        """
        command = ["/sbin/zfs", "list", "-H", self.target]
        try:
            with open("/dev/null", "w") as devnull:
                subprocess.check_call(command, stdout=devnull, stderr=devnull)
        except subprocess.CalledProcessError:
            raise ZFSBackupError("Target {} does not exist".format(self.target))
        # Okay, now let's start creating the dataset paths.  We create
        # them readonly, starting below the pool name.  We don't care if it fails; the
        # replication will fail later if we can't.
        full_path = self.target
        with open("/dev/null", "w+") as devnull:
            for d in self.dataset.split("/")[1:]:
                full_path = os.path.join(full_path, d)
                command = ["/sbin/zfs", "create", "-o", "readonly=on", full_path]
                print("Running command {}".format(" ".join(command)), file=sys.stderr)
                try:
                    subprocess.call(command, stdout=devnull, stderr=devnull)
                except:
                    pass
        return

    def destroy(self, snapname):
        """
        Destroy (recursively, if set) a given snapshot.
        """
        full_path = os.path.join(self.target, *(self.dataset.split("/")[1:]))
        command = ["/sbin/zfs", "destroy"]
        if self.recursive:
            command.append("-r")
        command.append("{}@{}".format(full_path, snapname))
        with open("/dev/null", "w+") as devnull:
            try:
                subprocess.check_call(command, stdout=devnull, stderr=devnull)
            except subprocess.CalledProcessError:
                raise ZFSBackupError("Unable to destroy snapshot {}@{}".format(full_path, snapname))
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
                subprocess.check_call(command, stdin=fobj, stderr=error_output)
            except subprocess.CalledProcessError:
                name = "{}@{}".format(self.dataset, snapname)
                error_output.seek(0)
                print("`{}` failed: {}".format(" ".join(command), error_output.read()),
                      file=sys.stderr)
                raise ZFSBackupError("Could not replicate {} to target {}".format(name, self.target))
        return
    
    @property
    def target(self):
        return self._dest

    @property
    def dataset(self):
        return self._ds

    @property
    def recursive(self):
        return self._recursive
    
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

class ZFSReplicationSSH(ZFSReplication):
    def __init__(self, target, dataset, recursive=False, ssh_cmd=None):
        pass
    
class ZFSReplicationCount(ZFSReplication):
    def __init__(self, target, dataset, recursive=False):
        super(ZFSReplicationCount, self).__init__(target, dataset, recursive)
        self._count = 0
        
    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.target)

    def validate(self):
        return

    def destroy(self, snapname):
        return
    
    def replicate(self, source, snapname, previous=None, date=int(time.time())):
        """
        Just count the characters
        """
        count = 0
        mByte = 1024 * 1024
        fobj = self._filter(source)
        while True:
            buf = fobj.read(mByte)
            if buf:
                count += len(buf)
            else:
                break
            
        self._count = count
        
    @property
    def snapshots(self):
        return []
    @property
    def count(self):
        return self._count
    
def main():
    import argparse

    def to_bool(s):
        if s.lower() in ("yes", "1", "true", "t", "y"):
            return True
        return False

    parser = argparse.ArgumentParser(description='ZFS snapshot replictor')
    parser.register('type', 'bool', to_bool)
    
    parser.add_argument('--recursive', '-R', dest='recursive',
                        type=bool,
                        default=False,
                        help='Recursively replicate')
    parser.add_argument('--snapshot', '-S', dest='snapshot_name',
                        required=True,
                        help='Snapshot to replicate')

    parser.add_argument("--compressed", "-C", dest='compressed',
                        action='store_true', default=False,
                        help='Compress snapshots')
    
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

    args = parser.parse_args()
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
        replicator = ZFSReplicationCount("<none>", dataset, recursive=args.recursive)
    elif args.subcommand == 'zfs':
        replicator = ZFSReplication(args.destination, dataset, recursive=args.recursive)
    else:
        print("Unknown replicator {}".format(args.subcommand), file=sys.stderr)
        sys.exit(1)


    if args.compressed:
        replicator.AddFilter("/usr/local/bin/pigz")
    command = ["/sbin/zfs", "send"]
    if args.recursive:
        command.append("-R")
    command.append("{}@{}".format(dataset, snapname))

    with tempfile.TemporaryFile() as error_output:
        with open("/dev/null", "w+") as devnull:
            mByte = 1024 * 1024
            snap_io = subprocess.Popen(command,
                                       bufsize=mByte,
                                       stdin=devnull,
                                       stderr=error_output,
                                       stdout=subprocess.PIPE)
            replicator.replicate(snap_io.stdout, snapname)
        if snap_io.returncode:
            error_output.seek(0)
            print("`{}` failed: {}".format(command, error_output.read()), file=sys.stderr)
            sys.exit(1)
    if args.subcommand == 'counter':
        print("Counted {} bytes".format(replicator.count))
        
if __name__ == "__main__":
    main()
