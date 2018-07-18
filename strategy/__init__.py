import os
import sys
import logging
import importlib.util
import inspect
from copy import deepcopy
from collections import OrderedDict
from typing import Dict, Optional, Type
from strategy_interface import IStrategy
dir_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(dir_root)
import conf
import util
import db
logger = util.get_log(__name__)


def load_strategy(strategy_name: str) -> IStrategy:
    try:
        for entry in os.listdir(conf.dir_user_strategies):
            if not entry.endswith('.py'):
                continue
            strategy = get_valid_strategies(
                os.path.abspath(os.path.join(conf.dir_user_strategies, entry)), strategy_name
            )
            if strategy:
                return import_strategy(strategy())
    except FileNotFoundError:
        return None
    return None

def get_valid_strategies(module_path: str, strategy_name: str) -> Optional[Type[IStrategy]]:
    # Generate spec based on absolute path
    spec = importlib.util.spec_from_file_location('unknown', module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)  # type: ignore # importlib does not use typehints
    valid_strategies_gen = (
        obj for name, obj in inspect.getmembers(module, inspect.isclass)
        if strategy_name == name and IStrategy in obj.__bases__
    )
    return next(valid_strategies_gen, None)

def import_strategy(strategy: IStrategy) -> IStrategy:
    attr = deepcopy({**strategy.__class__.__dict__, **strategy.__dict__})
    attr['__module__'] = 'strategy'
    name = strategy.__class__.__name__
    clazz = type(name, (IStrategy,), attr)
    logger.debug(
        'import_strategy %s.%s as %s.%s',
        strategy.__module__, strategy.__class__.__name__,
        clazz.__module__, strategy.__class__.__name__,
    )
    globals()[name] = clazz
    return clazz()
