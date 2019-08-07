import abc

import os
import sys
import time
import logging


class BaseAuthenticationServicer(abc.ABC):
    @abc.abstractmethod
    def authorize(self):
        raise NotImplementedError('Method not implemented!')

    @abc.


