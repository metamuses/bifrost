class Handler:
    def __init__(self):
        self.dbPathOrUrl = ""

    def getDbPathOrUrl(self):
        return self.dbPathOrUrl

    def setDbPathOrUrl(self, pathOrUrl):
        if isinstance(pathOrUrl, str):
            self.dbPathOrUrl = pathOrUrl
            return True
        else:
            raise ValueError("Path or URL must be a string")

class UploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        # Implemented in subclasses
        pass

class JournalUploadHandler(UploadHandler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        # TODO: Implement this method
        if not self.getDbPathOrUrl():
            raise ValueError("Database path or URL is not set")

        print(f"Pushing data to {self.getDbPathOrUrl()}")
        return True

class CategoryUploadHandler(Handler):
    def __init__(self):
        super().__init__()

    def pushDataToDb(self, path):
        # TODO: Implement this method
        if not self.getDbPathOrUrl():
            raise ValueError("Database path or URL is not set")

        print(f"Pushing data to {self.getDbPathOrUrl()}")
        return True

class QueryHandler(Handler):
    def __init__(self):
        super().__init__()

    def getById(self, id):
        # TODO: Implement this method
        pass

class JournalQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllJournals(self):
        # TODO: Implement this method
        pass

    def getJournalsWithTitle(self, partialTitle):
        # TODO: Implement this method
        pass

    def getJournalsPublishedBy(self, partialName):
        # TODO: Implement this method
        pass

    def getJournalsWithLicense(self, licenses):
        # TODO: Implement this method
        pass

    def getJournalsWithAPC(self):
        # TODO: Implement this method
        pass

    def getJournalsWithDOAJSeal(self):
        # TODO: Implement this method
        pass

class CategoryQueryHandler(QueryHandler):
    def __init__(self):
        super().__init__()

    def getAllCategories(self):
        # TODO: Implement this method
        pass

    def getAllAreas(self):
        # TODO: Implement this method
        pass

    def getCategoriesWithQuartile(self, quartiles):
        # TODO: Implement this method
        pass

    def getCategoriesAssignedToAreas(self, area_ids):
        # TODO: Implement this method
        pass

    def getAreasAssignedToCategories(self, category_ids):
        # TODO: Implement this method
        pass
