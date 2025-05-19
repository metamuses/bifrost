from impl.models import Category, Area, Journal
from impl.handlers import CategoryQueryHandler, JournalQueryHandler

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
        df = self.categoryQuery[0].getAllCategories()
        categories = []

        for index, row in df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)

        return categories

    def getAllAreas(self):
        df = self.categoryQuery[0].getAllAreas()
        areas = []

        for index, row in df.iterrows():
            area = Area(row["name"])
            areas.append(area)

        return areas

    def getCategoriesWithQuartile(self, quartiles):
        df = self.categoryQuery[0].getCategoriesWithQuartile(quartiles)
        categories = []

        for index, row in df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)

        return categories

    def getCategoriesAssignedToAreas(self, area_ids):
        df = self.categoryQuery[0].getCategoriesAssignedToAreas(area_ids)
        categories = []

        for index, row in df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)

        return categories

    def getAreasAssignedToCategories(self, category_ids):
        df = self.categoryQuery[0].getAreasAssignedToCategories(category_ids)
        areas = []

        for index, row in df.iterrows():
            area = Area(row["name"])
            areas.append(area)

        return areas

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
