import unittest

from meshweaver.core.errors import (
    MeshWeaverError,
    ConfigurationError,
    ProtocolError,
    StorageError,
    TaskError,
)


class TestSharedErrors(unittest.TestCase):

    def test_all_errors_inherit_from_base_error(self):
        self.assertTrue(issubclass(ConfigurationError, MeshWeaverError))
        self.assertTrue(issubclass(ProtocolError, MeshWeaverError))
        self.assertTrue(issubclass(StorageError, MeshWeaverError))
        self.assertTrue(issubclass(TaskError, MeshWeaverError))


if __name__ == "__main__":
    unittest.main()