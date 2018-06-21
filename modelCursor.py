class ModelCursor(object):
    """
    Cursor to iterate through documents of a consult. Documents must
    be return as Model Object.
    """

    def __init__(self, model_class, command_cursor):
        """ Initialize ModelCursor
        Parameters:
            model_class (class) -- class to create models of cursors.
            command_cursor (CommandCursor) -- pymongo's cursor.
        """
        self.__command_cursor__ = command_cursor
        self.model_class        = model_class

    def next(self):
        """ Returns the next document as a Model.
        """
        next_document = self.__command_cursor__.next()
        next_document = self.model_class(**next_document)
        return next_document

    @property
    def alive(self):
        """ True if there are models left, False otherwise.
        """
        return self.__command_cursor__.alive
