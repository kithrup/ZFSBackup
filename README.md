## Synopsis

ZFSBackup is a Python module to backup ZFS pools and datasets;
it's written in an object-oriented fashion to allow for different
backup methods.

## API Reference

The base class is ZFSBackup:

    backup = ZFSBackup("tank", "BackupPool/tank", recursive=True)

This creates an object which can be used to backup "tank" to "BackupPool/tank",
recursively.  The other classes are

* ZFSBackupSSH
* ZFSBackupDirectory
* ZFSBackupS3
* ZFSBackupCount

Backups are initiated by

    backup.backup(snapname=None, force_full=False)

Specifying a snapshot name will use that; otherwise it will use
the most recent snapshot.  Unless told not to, it will attempt
to do an incremental backup, by quering the targt for a list of
snapshots (how depends on which class is used).

### Filters

Backups can have filters applied, using the class ZFSBackupFilter.
The classes are

* ZFSBackupFilterThread
* ZFSBackupFilterCommand
* ZFSBackupFilterCompressed
* ZFSBackupFilterCounter

An example use:

    backup.AddFilter(ZFSBackupFilterCompressed())

which will interpose gzip between the source and target.  (Some
classes may ignore the filters, as they don't make sense.)  Using
ZFSBackupFilterCommand, any command can be used; the constructor
should be given an array of command and arguments -- one for
backing up, and one for restoring.  As an example:

    filter = ZFSBackupFilterCommand(backup_command=["/bin/cat"])

## Limitations

* Restore is not yet implemented.
* There is currently no way to monitor progress properly.
* Errors in the sub-processes and filters don't propagate.
