
class BaseFormatHandler:
    format_name = None
    extensions = []
    default_extension = None

    def sync(self, *args, **kwargs):
        raise NotImplementedError()

    def discover(self, *args, **kwargs):
        raise NotImplementedError()
