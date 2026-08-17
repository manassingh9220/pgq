_tasks : dict[str, callable] = {}

def task(name:str):
    """Register a function so worker can find it by name."""

    def deco(fn):
        if name in _tasks:
            raise ValueError(f"Task {name!r} already registered.")
        _tasks[name] = fn
        return fn
    return deco

def get(name:str):
    return _tasks.get(name)