import importlib
import sys

def log_module_versions(modules):
    """
    Reloads and logs the __version__ attribute of each module in the list.
    Useful for confirming that the latest version of external scripts is being used.
    
    Parameters:
        modules (list): List of module names (as strings) to reload and log.
    """
    for module_name in modules:
        if module_name in sys.modules:
            module = importlib.reload(sys.modules[module_name])
        else:
            module = importlib.import_module(module_name)

        version = getattr(module, "__version__", "❓ No __version__ attribute")
        print(f"✅ Module '{module_name}' loaded with version: {version}")
