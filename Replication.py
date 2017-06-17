from __future__ import print_function
import os, sys
import subprocess
import time
import tempfile

class ZFSReplicationError(ValueError):
    pass

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
            raise ZFSReplicationError("Target {} does not exist".format(self.target))
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

    def replicate(self, source, snapname, date=None):
        """
        Replicate from source.  source must be an object that supports
        read().  If date is not given, we will use the current time, so
        it should really be set.  The full snapshot name from the source
        would be dataset@snapname.

        The snapname and date parameters are for informational purposes only;
        the base class doesn't use them, but derived classes may.
        """
        destination = os.path.join(self.target, self.dataset)
        command = ["/sbin/zfs", "receive", "-d", "-F", self.target]
        with tempfile.TemporaryFile() as error_output:
            try:
                subprocess.check_call(command, stdin=source, stderr=error_output)
            except subprocess.CalledProcessError:
                name = "{}@{}".format(self.dataset, snapname)
                error_output.seek(0)
                print("`{}` failed: {}".format(" ".join(command), error_output.read()),
                      file=sys.stderr)
                raise ZFSReplicationError("Could not replicate {} to target {}".format(name, self.target))
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

class ZFSReplicationCount(ZFSReplication):
    def __init__(self, target, dataset, recursive=False):
        super(ZFSReplicationCount, self).__init__(target, dataset, recursive)
        self._count = 0
        
    def __repr__(self):
        return "{}({})".format(self.__class__.__name__, self.target)

    def validate(self):
        return

    def replicate(self, source, snapname, date=None):
        """
        Just count the characters
        """
        count = 0
        mByte = 1024 * 1024
        while True:
            buf = source.read(mByte)
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
        replicator = ZFSReplicationCount("", dataset, recursive=args.recursive)
    elif args.subcommand == 'zfs':
        replicator = ZFSReplication(args.destination, dataset, recursive=args.recursive)
    else:
        print("Unknown replicator {}".format(args.subcommand), file=sys.stderr)
        sys.exit(1)


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
    sys.exit(0)
    # Should not hardcode this, even for testing.
    (dataset, snapname) = "zroot/usr/home@auto-daily-2017-06-17".split('@')
    for ds in sys.argv[1:]:
        target = ZFSReplication(ds, dataset)
#        print("Target = {}".format(target))
#        print(target.snapshots)
        target = ZFSReplicationCount(ds, dataset)
        print("Target = {}".format(target))
        command = ["/sbin/zfs", "send", "{}@{}".format(dataset, snapname)]

        with tempfile.TemporaryFile() as error_output:
            with open("/dev/null", "rw") as devnull:
                mByte = 1024 * 1024
                snap_io = subprocess.Popen(command,
                                           bufsize=mByte,
                                           stdin=devnull,
                                           stderr=error_output,
                                           stdout=subprocess.PIPE)
                target.replicate(snap_io.stdout, snapname)
            
            if snap_io.returncode:
                error_output.seek(0)
                print("`{}` failed {}: {}".format(command, snap_io.returncode, error_output.read()), file=sys.stderr)
            else:
                print("{} bytes in snapshot".format(target.count))
