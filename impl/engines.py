class BasicQueryEngine:
    def __init__(self):
        self.journalQuery = []
        self.categoryQuery = []

    def cleanJournalHandlers(self):
        self.journalQuery.clear()
        return True

    def cleanCategoryHandlers(self):
        self.categoryQuery.clear()
        return True

    def addJournalHandler(self, handler):
        self.journalQuery.append(handler)
        return True

    def addCategoryHandler(self, handler):
        self.categoryQuery.append(handler)
        return True

    def getEntityById(self, id):
        # TODO: Implement this class
        pass

    def getAllJournals(self):
        # TODO: Implement this class
        pass

    def getJournalsWithTitle(self, partialTitle):
        # TODO: Implement this class
        pass

    def getJournalsPublishedBy(self, partialName):
        # TODO: Implement this class
        pass

    def getJournalsWithLicense(self, licenses):
        # TODO: Implement this class
        pass

    def getJournalsWithAPC(self):
        # TODO: Implement this class
        pass

    def getJournalsWithDOASeal(self):
        # TODO: Implement this class
        pass

    def getAllCategories(self):
        # TODO: Implement this class
        pass

    def getAllAreas(self):
        # TODO: Implement this class
        pass

    def getCategoriesWithQuartile(self, quartiles):
        # TODO: Implement this class
        pass

    def getCategoriesAssignedToAreas(self, area_ids):
        # TODO: Implement this class
        pass

    def getAreasAssignedToCategories(self, category_ids):
        # TODO: Implement this class
        pass

class FullQueryEngine(BasicQueryEngine):
    def getJournalsInCategoriesWithQuartile(self, category_ids, quartiles):
        # TODO: Implement this class
        pass

    def getJournalsInAreasWithLicense(self, areas_ids, licenses):
        # TODO: Implement this class
        pass

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas_ids, category_ids, quartiles):
        # TODO: Implement this class
        pass
