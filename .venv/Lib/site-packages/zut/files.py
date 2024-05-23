"""
File and directory operations, compatible with both the local file system and Samba/Windows shares.
"""
from __future__ import annotations

import logging
import ntpath
import os
import shutil
import sys
from datetime import datetime
from pathlib import Path
from typing import Callable

try:
    # package `smbprotocol`: required if not Windows, or if non default credentials
    import smbclient
    import smbclient.path as smbclient_path
    import smbclient.shutil as smbclient_shutil
except ImportError:
    smbclient = None
    smbclient_path = None
    smbclient_shutil = None

_open = open
_smb_credentials_configured = False

logger = logging.getLogger(__name__)


def can_use_network_paths():
    if sys.platform == 'win32' and not _smb_credentials_configured:
        return True  # Python is natively compatible with Samba shares on Windows

    return smbclient is not None


def _standardize(path: str) -> tuple[str,bool]:
    """
    Return (path, use_native).
    """
    if not path:
        return path, True
    
    if isinstance(path, Path):
        path = str(path)

    path = os.path.expanduser(path)
    
    if not (path.startswith("\\\\") or path.startswith("//")):
        return path, True  # not a network path
        
    if sys.platform == 'win32' and not _smb_credentials_configured:
        return path, True  # Python is natively compatible with Samba shares on Windows

    return path, False


def dirname(path: str):
    path, use_native = _standardize(path)
    
    if use_native:
        return os.path.dirname(path)
    
    return ntpath.dirname(path)


def basename(path: str):
    path, use_native = _standardize(path)

    if use_native:
        return os.path.basename(path)

    return ntpath.basename(path)
    

def splitext(path: str):
    path, use_native = _standardize(path)

    if use_native:
        return os.path.splitext(path)
    
    return ntpath.splitext(path)


def exists(path: str):
    path, use_native = _standardize(path)

    if use_native:
        return os.path.exists(path)

    if not smbclient:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    return smbclient_path.exists(path)


def stat(path: str):
    path, use_native = _standardize(path)

    if use_native:
        return os.stat(path)
    
    if not smbclient:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    return smbclient.stat(path)


def makedirs(path: str, exist_ok: bool = False):
    path, use_native = _standardize(path)

    if use_native:
        return os.makedirs(path, exist_ok=exist_ok)

    if not smbclient:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    return smbclient.makedirs(path, exist_ok=exist_ok)


def remove(path: str, missing_ok: bool = False):
    path, use_native = _standardize(path)

    if missing_ok:
        if not exists(path):
            return

    if use_native:
        os.remove(path)
        return

    if not smbclient:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    smbclient.remove(path)


def rmtree(path: str, ignore_errors=False, onerror=None, missing_ok: bool = False):
    path, use_native = _standardize(path)

    if missing_ok:
        if not exists(path):
            return

    if use_native:
        shutil.rmtree(path, ignore_errors=ignore_errors, onerror=onerror)
        return
    
    if not smbclient_shutil:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    smbclient_shutil.rmtree(path, ignore_errors=ignore_errors, onerror=onerror)


def open(path: str, mode="r", buffering: int = -1, encoding: str = None, errors: str = None, newline: str = None, mkdir: bool = False, **kwargs):
    if mkdir:
        dir_path = dirname(path)
        if dir_path:
            makedirs(dir_path, exist_ok=True)

    path, use_native = _standardize(path)

    if use_native:
        return _open(path, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline, **kwargs)

    if not smbclient:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    return smbclient.open_file(path, mode=mode, buffering=buffering, encoding=encoding, errors=errors, newline=newline, **kwargs)


def read_bytes(path: str):
    """
    Open the file in bytes mode, read it, and close the file.
    """
    with open(path, mode='rb') as f:
        return f.read()


def read_text(path: str, encoding: str = None, errors: str = None):
    """
    Open the file in text mode, read it, and close the file.
    """
    with open(path, mode='r', encoding=encoding, errors=errors) as f:
        return f.read()


def write_bytes(path: str, data):
    """
    Open the file in bytes mode, write to it, and close the file.
    """
    with open(path, mode='wb') as f:
        return f.write(data)


def write_text(path: str, data: str, encoding: str = None, errors: str = None, newline: str = None):
    """
    Open the file in text mode, write to it, and close the file.
    """
    with open(path, mode='w', encoding=encoding, errors=errors, newline=newline) as f:
        return f.write(data)


def copy(src: str, dst: str, follow_symlinks=True):
    """
    Copy file data and file data and file's permission mode (which on Windows is only the read-only flag).
    Other metadata like file's creation and modification times, are not preserved.

    The destination may be a directory (in this case, the file will be copied into `dst` directory using
    the base filename from `src`).

    If `follow_symlinks` is `False`, `dst` will be created as a symbolic link if `src` is a symbolic link.
    If `follow_symlinks` is `True`, `dst` will be a copy of the file `src` refers to.
    """
    src, src_native = _standardize(src)
    dst, dst_native = _standardize(dst)
    
    if src_native and dst_native:
        return shutil.copy(src, dst, follow_symlinks=follow_symlinks)
    
    if not smbclient_shutil:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    return smbclient_shutil.copy(src, dst, follow_symlinks=follow_symlinks)


def copy2(src: str, dst: str, follow_symlinks=True):
    """
    Identical to `copy()` except that `copy2()` also attempts to preserve the file metadata.

    `copy2()` uses `copystat()` to copy the file metadata. Please see `copystat()` for more information about how and what
    metadata it copies to the `dst` file.

    If `follow_symlinks` is `False`, `dst` will be created as a symbolic link if `src` is a symbolic link.
    If `follow_symlinks` is `True`, `dst` will be a copy of the file `src` refers to.
    """
    src, src_native = _standardize(src)
    dst, dst_native = _standardize(dst)
    
    if src_native and dst_native:
        return shutil.copy2(src, dst, follow_symlinks=follow_symlinks)
    
    if not smbclient_shutil:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    return smbclient_shutil.copy2(src, dst, follow_symlinks=follow_symlinks)


def copyfile(src: str, dst: str, follow_symlinks=True):
    """
    Copy the contents (no metadata) in the most efficient way possible.

    If `follow_symlinks` is `False`, `dst` will be created as a symbolic link if `src` is a symbolic link.
    If `follow_symlinks` is `True`, `dst` will be a copy of the file `src` refers to.
    """
    src, src_native = _standardize(src)
    dst, dst_native = _standardize(dst)
    
    if src_native and dst_native:
        return shutil.copyfile(src, dst, follow_symlinks=follow_symlinks)
    
    if not smbclient_shutil:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    return smbclient_shutil.copyfile(src, dst, follow_symlinks=follow_symlinks)


def copystat(src: str, dst: str, follow_symlinks=True):
    """
    Copy the read-only attribute, last access time, and last modification time from `src` to `dst`.
    The file contents, owner, and group are unaffected.

    If `follow_symlinks` is `False` and `src` and `dst` both refer to symbolic links, the attributes will be read and written
    on the symbolic links themselves (rather than the files the symbolic links refer to).
    """
    src, src_native = _standardize(src)
    dst, dst_native = _standardize(dst)
    
    if src_native and dst_native:
        return shutil.copystat(src, dst, follow_symlinks=follow_symlinks)
    
    if not smbclient_shutil:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    return smbclient_shutil.copystat(src, dst, follow_symlinks=follow_symlinks)


def copymode(src: str, dst: str, follow_symlinks=True):
    """
    Copy the permission bits from `src` to `dst`.
    The file contents, owner, and group are unaffected.
    
    Due to the limitations of Windows, this function only sets/unsets `dst` FILE_ATTRIBUTE_READ_ONLY flag based on what `src` attribute is set to.

    If `follow_symlinks` is `False` and `src` and `dst` both refer to symbolic links, the attributes will be read and written
    on the symbolic links themselves (rather than the files the symbolic links refer to).
    """
    src, src_native = _standardize(src)
    dst, dst_native = _standardize(dst)

    if src_native and dst_native:
        return shutil.copymode(src, dst, follow_symlinks=follow_symlinks)

    if not smbclient_shutil:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    return smbclient_shutil.copymode(src, dst, follow_symlinks=follow_symlinks)


def copytree(src: str, dst: str, symlinks: bool = False, ignore: Callable[[str, list[str]],list[str]] = None, ignore_dangling_symlinks: bool = False, dirs_exist_ok: bool = False):
    """
    Recursively copy a directory tree rooted at `src` to a directory named `dst` and return the destination directory.

    Permissions and times of directories are copied with `copystat()`, individual files are copied using `copy2()`.

    If `symlinks` is true, symbolic links in the source tree result in symbolic links in the destination tree;
    if it is false, the contents of the files pointed to by symbolic links are copied. If the file pointed by the symlink doesn't
    exist, an exception will be added. You can set `ignore_dangling_symlinks` to true if you want to silence this exception.
    Notice that this has no effect on platforms that don't support `os.symlink`.

    If `dirs_exist_ok` is false (the default) and `dst` already exists, an error is raised. If `dirs_exist_ok` is true, the copying
    operation will continue if it encounters existing directories, and files within the `dst` tree will be overwritten by corresponding files from the
    `src` tree.

    If `ignore` is given, it must be a callable of the form `ignore(src, names) -> ignored_names`.
    It will be called recursively and will receive as its arguments the directory being visited (`src`) and a list of its content (`names`).
    It must return a subset of the items of `names` that must be ignored in the copy process.
    """
    src, src_native = _standardize(src)
    dst, dst_native = _standardize(dst)

    if src_native and dst_native:
        return shutil.copytree(src, dst, symlinks=symlinks, ignore=ignore, ignore_dangling_symlinks=ignore_dangling_symlinks, dirs_exist_ok=dirs_exist_ok)

    if not smbclient_shutil:
        raise ModuleNotFoundError(f'missing package `smbprotocol`')
    return smbclient_shutil.copytree(src, dst, symlinks=symlinks, ignore=ignore, ignore_dangling_symlinks=ignore_dangling_symlinks, dirs_exist_ok=dirs_exist_ok)


def archivate(path: str|Path, target: str|Path = None, *, missing_ok: bool = False) -> str:
    """
    Copy `path` to `target` directory, ensuring unique subdir name.
    """
    if isinstance(path, Path):
        path = str(path)


    if missing_ok:
        if not path or not exists(path):
            return

    if isinstance(target, Path):
        target = str(target)
    
    if not exists(path):
        return ValueError(f'path does not exist: {path}')
    
    if target and not exists(target):
        makedirs(target)
   
    st = stat(path)
    mtime = datetime.fromtimestamp(st.st_mtime)

    bname = basename(path)
    stem, ext = splitext(bname)
    mainpart = (target if target else dirname(path)) + '/' + stem + f"_{mtime.strftime('%Y%m%d')}"
    
    i = 1
    while True:
        archive = mainpart + (f"-{i}" if i > 1 else '') + ext
        if not exists(archive):
            break
        i += 1

    logger.info(f"Archivate {path} to {archive}")
    copy2(path, archive)
    return archive
