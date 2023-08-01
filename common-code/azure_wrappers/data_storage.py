import base64
import functools
import hashlib
import io
import os
import pickle
import socket
from logging import Logger
from pathlib import Path

from appdirs import AppDirs
from azure.core.exceptions import ResourceNotFoundError
from cryptography.fernet import Fernet, InvalidToken

from azure_box.data_parsing import parse_data_source

LOGGER = Logger(__file__)


class DataSource:

    """This class is intended to be a minimal implementation of file parsing.
    Most data parsing logic should be part of data_parsing.parse_data. The main
    problem this class seeks to solve is that parsing an excel file takes a very
    long time. We wish to cache that parsing and store the resulting dataframe
    locally. It is conceivable that a similar approach will be implemented in
    the future for other tricky file formats. The intended behavior is:
    1. If no caches exist fetch the data from Azure, store it locally via the
       CachedStream class, parse the data (storing the result locally in the
       case of excel), and return to the user.
    2. On subequent calls the local and parsed excel data will be returned and
       for all other files the locally stored raw stream will be parsed and returned
       to the user.

    """

    def __init__(
        self, container_client, file_name, return_stream=None, **cached_stream_kwargs
    ):
        # Set public attributes
        self.file_name = file_name
        self.version_id = cached_stream_kwargs.get("version_id")
        self.return_stream = return_stream
        self.container_client = container_client
        self.cached_stream_kwargs = cached_stream_kwargs

        # Some attributes not meant for general usage.
        self.__cached_stream = None
        self._data = None
        self._stream = None
        self._cachefile = None

    def __repr__(self):
        """Returns the default representation of the class instance during interactive usage."""
        cs_kwargs = ", ".join(
            f"{key}={value}" for key, value in self.cached_stream_kwargs.items()
        )
        return (
            f"{self.__class__.__name__}"
            f"(container_client={self.container_client!r}, file_name={self.file_name!r},{cs_kwargs})"
        )

    @property
    def data(self):
        """Provides the parsed data to the user as defined in data_parsing.parse_data."""
        if self._data is None:

            if not self._cached_stream.exists():
                raise ResourceNotFoundError(
                    f"Cannot find '{self.file_name}' in '{self.container_client.container_name}'."
                )

            if self.cachefile is not None and self.cachefile.exists():
                try:
                    self._data = self._read_cache()
                except InvalidToken:
                    LOGGER.warning(
                        f"Cache retrieval ({self.cachefile}) failed with the key that was "
                        "expected to work. Trying to regenerate cache..."
                    )
                else:
                    # return successfully loaded cache
                    return self._data

            # Cache is non existent or unreadable:
            self._data = parse_data_source(
                self.file_name,
                self._cached_stream,
                self.return_stream,
            )
            self._store_cache()
        return self._data

    @property
    def stream(self):
        """Public attribute meant for downstream usage if access to data streaming
        is required."""
        raise NotImplementedError

    @property
    def cachefile(self):
        """Not meant for typical usage. Should only return a value if the file
        format has caching enabled for the parsed object i.e. excel files. The
        cachefile is computed as a derivative of the path that points to the
        download cache for the current file."""
        if not "xls" in str(self.file_name):
            return None

        if self._cachefile is None:
            stream_cachefile = self._cached_stream.cachefile
            self._cachefile = Path(
                str(stream_cachefile).replace(".crypt", ".parsed.crypt")
            )
        return self._cachefile

    @property
    def _cached_stream(self):
        """Instantiates a cached stream object to handle download caching."""
        if self.__cached_stream is None:
            self.__cached_stream = SecureCachedStream(
                self.container_client,
                self.file_name,
                **self.cached_stream_kwargs,
            )
        return self.__cached_stream

    def _read_cache(self):
        """Reads a python object from disk in encrypted form. Not meant for typical
        usage but useful for files that take a long time to parse and so a
        cached download is still inadequate for convenient interactive
        iteration."""
        if not "xls" in str(self.file_name):
            raise ValueError(
                f"No local cache for a parsed version of {self.file_name}. "
                "A local cache of the parsed data is only stored for excel files."
            )
        parsed_data = decrypt_object_from_file(self._cached_stream._key, self.cachefile)
        return parsed_data

    def _store_cache(self):
        """See _read_cache"""
        if "xls" in str(self.file_name):
            encrypt_object_to_file(self._data, self._cached_stream._key, self.cachefile)


class SecureCachedStream:
    """Intended to roughly emulate the behavior of the blob stream object
    returned when fetching data from Azure cloud. This could be adapted for
    other cloud providers as required. When data is downloaded it is stored
    locally on disk in encrypted form. The filename used for the cache is
    derived from the content cache trigger cache invalidation when the file
    stored in the cloud changes. The key used for encryption is specific to the
    host, user, and cloud data container, is generated as required, and is never
    stored locally."""

    def __init__(
        self,
        container_client,
        file_name,
        version_id=None,
        key=None,
        root_cachedir=None,
        store_stream=True,
    ):
        """Initalize the SecureDataStream for a given file stored in a container
        referred to by the container_client object.

        Args:
            container_client (azure.storage.blob.ContainerClient): An authenticated container client that contains the file
            file_name (str): The location of the data file.
            version_id (str, optional): A specific version of the file
            store_stream (bool, optional): Whether to cache the contents of the blob to file when accessing it via the stream method.
            root_cachedir (pathlib.Path, optional): A directory to store and retrieve encrypted caches.
            key (bytes): Not intended for general use.
        """

        # Do not instantiate for a missing container
        try:
            next(container_client.list_blobs())
        except ResourceNotFoundError as err:
            print(err)
            raise ResourceNotFoundError(
                "No blobs were found in the container "
                f"'{container_client.container_name}'. Does it exist?"
            )

        # Set attributes
        self.container_client = container_client
        self.file_name = file_name
        self.version_id = version_id
        self.__key = key
        if root_cachedir is None:
            root_cachedir = get_cachedir("pipelines")
        self.root_cachedir = Path(root_cachedir)
        self.store_stream = store_stream
        self.__data = None
        self.__keyname = None
        self._stream = None
        self._cachefile = None
        self.__blob_client = None
        self.__blob_props = None

    def __repr__(self):
        return (
            f"{self.__class__.__name__}"
            f"(container_client={self.container_client!r}, file_name={self.file_name!r})"
        )

    def readall(self):
        """This method is used by other classes whose behavior we wish to emulate...

        Returns:
            bytes: The full content of the file stored locally or in the cloud as a bytes object.
        """
        return self._data

    def readinto(self, *args, **kwargs):
        """Exposes the io.BytesIO readinto method directly in the namespace of the class instance."""
        return self.stream.readinto(*args, **kwargs)

    def readline(self, *args, **kwargs):
        """Exposes the BytesIO readline method directly in the namespace of the class instance."""
        return self.stream.readline(*args, **kwargs)

    def exists(self):
        return self._blob_client.exists()

    @property
    def cachefile(self):
        if self._cachefile is None:
            self._cachefile = get_cached_filepath(
                self._blob_props, self.root_cachedir, self._keyname
            )
        return self._cachefile

    @property
    def _key(self):
        """Attribute that returns a symmetric key as a string of bytes. If a key
        was previously generated for a particular host/user/data-container
        combination it will be stored in the data-container and should be
        fetched for decryption of other data files previously stored.

        Returns:
            bytes: i.e. the output of cryptography.fernet.Fernet.generate_key(),
            or similar stored remotely.
        """
        if self.__key is None:
            key = fetch_or_create_key(self.container_client, self._keyname)
            self.__key = key
        return self.__key

    @property
    def _keyname(self):
        if self.__keyname is None:
            user = get_user_string()
            host = os.environ.get("ENCRYPTION_KEY_HOST") or socket.gethostname()

            self.__keyname = get_encryption_key_name(
                host,
                user,
                self.container_client.container_name,
            )
        return self.__keyname

    @property
    def _data(self):
        """
        Fetches and returns the full dataset as a string of bytes."""
        if self.__data is None:
            self.__data = self.stream.read()
        return self.__data

    @property
    def _blob_client(self):
        if self.__blob_client is None:
            self.__blob_client = self.container_client.get_blob_client(self.file_name)
        return self.__blob_client

    @property
    def _blob_props(self):
        if not self.__blob_props:
            self.__blob_props = self._blob_client.get_blob_properties(
                version_id=self.version_id
            )
        return self.__blob_props

    @property
    def stream(self):
        if not self._stream:
            if not self.root_cachedir.exists():
                self.root_cachedir.mkdir(parents=True, exist_ok=True)
            try:
                self._stream = io.BytesIO(
                    decrypt_bytes_from_file(self._key, self.cachefile)
                )
            except (FileNotFoundError, AttributeError):
                if self.cachefile is not None:
                    LOGGER.info(
                        f"Could not find a cachefile with the value {self.cachefile}. "
                        "Downloading and caching instead..."
                    )
            except InvalidToken:
                LOGGER.warning(
                    f"Cache retrieval ({self.cachefile}) failed with the key that was "
                    "expected to work. Trying to regenerate cache..."
                )
            else:
                return self._stream

            stream = self._blob_client.download_blob(version_id=self.version_id)
            if self.store_stream:
                # TODO: this could be sped up by using download_to_stream
                # with a seekable file or other object. One could also use
                # asyncio to stream to the file and the consumer of the
                # class instance simulatenously.
                encrypt_bytes_to_file(stream.readall(), self._key, self.cachefile)
            # TODO: the following fails for some files... debugging
            # this could speed  initial processing of files by
            # avoiding decrypting the file from disk.
            # self._stream = io.BytesIO()
            # stream.readinto(self._stream)
            # self._stream.seek(0)
            self._stream = io.BytesIO(
                decrypt_bytes_from_file(self._key, self.cachefile)
            )
        return self._stream


def generate_key():
    """Returns a key for symmetric encryption in byte form."""
    return Fernet.generate_key()


def get_encryption_key_name(host, user, container):
    """Returns a string representation of a b64 encoding of the key generation
    context. The cache is invalidated if this name is changed so a cache is
    considered specific to a host/user/container combination

    Suggested usage: import socket, os
    get_encryption_key_name(socket.gethostname(),os.getuid(),"container_name")

    Args:
        host (str): Name of current host
        user (str): A string describing the current user (uid is fine). Note
        os.getuid does not work on Windows.
        container (str): Name of the data container... likely
        container_client.container_name

    Returns:
        str: A string generated using base64 encoding of the input used to
        identify a key used for encryption
    """
    # create a key that is unique for the user/data container combination
    keyuid = f"{host}{user}{container}".encode("utf-8")
    # hash used to prevent keyname clashes
    keyname = hashlib.sha256(keyuid).hexdigest()
    return f"key/{keyname}"


def get_cached_filepath(blob_props, rootdir, key_name):
    """
    Given the properties object of an azure blob and the name of a key used for
    encryption, compute a filename for fetching or storing an encrypted local
    version of the data.

    Args:
        blob_props (azure.storage.blob.ContainerClient): The properties object
        for a particular blob
        rootdir (pathlib.Path): Root directory of where the cache should be
        located
        key_name (str): The name of the key used to encrypt the data. This helps
        to invalidate caches that were created with a key that is currently not
        available in the current context

    Returns:
        pathlib.Path: A pathlib object referring to a file that refers to the
        cache for the current data version regardless of the files existence.
    """
    version = (
        base64.b64encode(
            blob_props["content_settings"].get("content_md5") or b"no version"
        ).decode("utf-8")
        + f"_{key_name[-10:]}"
    )
    version_cleaned = "".join([c for c in version if c not in " %:/,.\\[]<>*?=+"])
    data_key = f"{blob_props['name']}_{version_cleaned}"
    return rootdir / f"{data_key}.crypt"


def encrypt_input(bytes_in, key):
    """Implements symmetric encryption with a Fernet key.
    The key should be created with:
        from cryptography.fernet import Fernet
        key = Fernet.generate_key()
    See the following for more info:
    https://cryptography.io/en/latest/
    """
    return Fernet(key).encrypt(bytes_in)


def decrypt_input(crypted, key):
    """Decrypts symmetrically encrypted data. The data should be encrypted
    bytes that are the output of the encrypt_input function. See that function
    for more info."""
    return Fernet(key).decrypt(crypted)


def encrypt_bytes_to_file(bytes_in, key, fpath):
    """_summary_

    Args:
        bytes_in (bytes): A byte string e.g. b"some data"
        key (bytes): A symmetric cryptographic key in byte form e.g. cryptography.fernet.Fernet.generate_key()
        fpath (pathlib.Path): A path to the file for storing the encrypted data.
    """
    crypted = encrypt_input(bytes_in, key)
    fpath.parent.mkdir(parents=True, exist_ok=True)
    fpath.write_bytes(crypted)


def decrypt_bytes_from_file(key, fpath):
    """Decrypt the bytes contained in a locally stored file

    Args:

        key (bytes): A symmetric cryptographic key in byte form e.g. cryptography.fernet.Fernet.generate_key()
        fpath (pathlib.Path): A path to the file for storing the encrypted data.

    Returns:
        bytes: Decrypted contents of a file as a bytes object.
    """
    crypted = fpath.read_bytes()
    return decrypt_input(crypted, key)


def encrypt_object_to_file(python_obj, key, fpath):
    """Pickles a python object such as a dataframe in memory into bytes, encrypts those bytes, and then writes the result to disk."""
    return encrypt_bytes_to_file(pickle.dumps(python_obj), key, fpath)


def decrypt_object_from_file(key, fpath):
    """Reads the bytes from a file, decrypts them, and uses pickle use the bytes
    to reconstitute the python object they represent into an object in memory
    which is then returned."""
    return pickle.loads(decrypt_bytes_from_file(key, fpath))


def get_cachedir(appname):
    """Returns a cross-platform data directory unless environment variables
    are set to modify the behavior during K8s deployment."""
    if os.environ.get("HOME_AS_CACHEDIR") is not None:
        if os.environ.get("DESIRED_HOME_FILEPATH") is not None:
            home_file_path = Path(os.environ.get("DESIRED_HOME_FILEPATH"))
            cachedir = home_file_path / f"storage/.cache/{appname}/YOUR-ORG"
        else:
            raise EnvironmentError(
                "DESIRED_HOME_FILEPATH must be set when HOME_AS_CACHEDIR is set"
            )
    else:
        dirs = AppDirs(appname, "YOUR-ORG")
        cachedir = dirs.user_cache_dir

    return str(cachedir)


@functools.lru_cache
def fetch_or_create_key(container_client, keyname):
    try:
        key = container_client.get_blob_client(keyname).download_blob().readall()
    except ResourceNotFoundError:
        key = generate_key()
        container_client.upload_blob(name=keyname, data=key, overwrite=True)
    return key


def get_user_string():
    try:
        user = os.getuid()
    except AttributeError:
        user = "likely_a_windows_user"
    return user
