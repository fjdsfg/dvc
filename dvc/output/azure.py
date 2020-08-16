from dvc.output.base import BaseOutput

from ..tree.azure import AzureTree


class AzureOutput(BaseOutput):
    TREE_CLS = AzureTree
