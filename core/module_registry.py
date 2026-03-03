import importlib.util
import logging
from dataclasses import dataclass
from pathlib import Path


@dataclass
class ModuleInfo:
    name: str
    path: str
    version: str
    healthy: bool
    reason: str
    instance: object


class ModuleRegistry:
    def __init__(self, modules_dir: Path):
        self.modules_dir = modules_dir
        self.registry = {}
        self.logger = logging.getLogger("discordbot.module_registry")
        self.dependencies = {
            "automod": ["logging"],
        }

    def discover(self):
        infos = {}
        for file in sorted(self.modules_dir.glob("*.py")):
            if file.name.startswith("__"):
                continue
            name = file.stem
            spec = importlib.util.spec_from_file_location(f"modules.{name}", str(file))
            if not spec or not spec.loader:
                infos[name] = ModuleInfo(name, str(file), "0.0.0", False, "spec load failed", None)
                continue
            try:
                mod = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(mod)
                version = str(getattr(mod, "VERSION", "1.0.0"))
                infos[name] = ModuleInfo(name, str(file), version, True, "ok", mod)
            except Exception as e:
                self.logger.warning("module_load_failed module=%s path=%s", name, str(file), exc_info=True)
                infos[name] = ModuleInfo(name, str(file), "0.0.0", False, f"{type(e).__name__}: {e}", None)
        self.registry = infos
        return infos

    def resolve(self):
        for name, deps in self.dependencies.items():
            info = self.registry.get(name)
            if not info or not info.healthy:
                continue
            for dep in deps:
                dep_info = self.registry.get(dep)
                if not dep_info or not dep_info.healthy:
                    info.healthy = False
                    info.reason = f"missing dependency: {dep}"
        return self.registry

    def health(self):
        return {
            name: {
                "version": info.version,
                "healthy": info.healthy,
                "reason": info.reason,
            }
            for name, info in self.registry.items()
        }

    def get(self, name):
        info = self.registry.get(name)
        if not info or not info.healthy:
            return None
        return info.instance

    def hot_reload(self):
        self.discover()
        self.resolve()
        return self.health()
