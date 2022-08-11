from feature_factory.framework.feature_factory.helpers import Helpers
from feature_factory.framework.config_obj import ConfigObj


feat_func = Helpers()._register_feature_func()
joiner_func = Helpers()._register_joiner_func()


# ADD DOCS HERE

class SalesCommon:
    def __init__(self, config=ConfigObj()):
        # FeatureFamily.__init__(self, config)
        self.config = config
        self._joiner_func = joiner_func
        self._feat_func = feat_func
        # FeatureFamily.__init__(self, config)

    def transfer_features(self, cls):
        for fn, func in self._feat_func.all.items():
            setattr(cls, fn, func)
