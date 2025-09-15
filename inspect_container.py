import testcontainers
import pkgutil
import inspect

def find_class(module_name, class_name):
    """Recursively search for a class in a module and its submodules."""
    try:
        module = __import__(module_name, fromlist=[''])
        for name, obj in inspect.getmembers(module):
            if inspect.isclass(obj) and obj.__name__ == class_name:
                print(f"Found class '{class_name}' in module '{module.__name__}'")
                return
            if inspect.ismodule(obj) and obj.__name__.startswith(module_name):
                find_class(obj.__name__, class_name)
    except ImportError:
        pass

# Start searching from the top-level 'testcontainers' package
find_class("testcontainers", "LogMessageWaitStrategy")

# Alternative: just print all submodules to find the 'wait' module
import testcontainers.core
for loader, module_name, is_pkg in pkgutil.walk_packages(testcontainers.core.__path__, testcontainers.core.__name__ + '.'):
    print(module_name)
