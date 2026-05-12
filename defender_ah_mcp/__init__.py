try:
    from importlib.metadata import version

    __version__ = version("defender-advanced-hunting-mcp")
except Exception:
    __version__ = "0.0.0.dev0"
