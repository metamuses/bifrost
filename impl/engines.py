from impl.models import Category, Area, Journal
from impl.handlers import CategoryQueryHandler, JournalQueryHandler

import pandas as pd

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
        all_dfs = [query.getById(id) for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()
        if not merged_df.empty:
            return self.buildJournal(merged_df.iloc[0])
        else:
            all_dfs = [query.getById(id) for query in self.categoryQuery]
            merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()
            if merged_df.empty:
                return None
            else:
                row = merged_df.iloc[0]
                if row["model"] == "area":
                    return Area(row["name"])
                if row["model"] == "category":
                    return Category(row["name"], row["quartile"])
                if row["model"] == "journal":
                    identifier = ",".join([row["identifier_1"], row["identifier_2"]])
                    dict = {"identifier": identifier, "title": "", "languages": "",
                            "publisher": None, "seal": False, "licence": "", "apc": False}
                    return self.buildJournal(dict)

    def getAllJournals(self):
        all_dfs = [query.getAllJournals() for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsWithTitle(self, partialTitle):
        all_dfs = [query.getJournalsWithTitle(partialTitle) for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsPublishedBy(self, partialName):
        all_dfs = [query.getJournalsPublishedBy(partialName) for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsWithLicense(self, licenses):
        all_dfs = [query.getJournalsWithLicense(licenses) for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsWithAPC(self):
        all_dfs = [query.getJournalsWithAPC() for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsWithDOAJSeal(self):
        all_dfs = [query.getJournalsWithDOAJSeal() for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        journals = []
        for index, row in merged_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getAllCategories(self):
        all_dfs = [query.getAllCategories() for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        categories = []
        for index, row in merged_df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)

        return categories

    def getAllAreas(self):
        all_dfs = [query.getAllAreas() for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        areas = []
        for index, row in merged_df.iterrows():
            area = Area(row["name"])
            areas.append(area)

        return areas

    def getCategoriesWithQuartile(self, quartiles):
        all_dfs = [query.getCategoriesWithQuartile(quartiles) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        categories = []
        for index, row in merged_df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)

        return categories

    def getCategoriesAssignedToAreas(self, area_ids):
        all_dfs = [query.getCategoriesAssignedToAreas(area_ids) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        categories = []
        for index, row in merged_df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)

        return categories

    def getAreasAssignedToCategories(self, category_ids):
        all_dfs = [query.getAreasAssignedToCategories(category_ids) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        areas = []
        for index, row in merged_df.iterrows():
            area = Area(row["name"])
            areas.append(area)

        return areas

    def buildJournal(self, row):
        ids = [item for item in row["identifier"].split(",") if item]
        title = row["title"]
        languages = [item for item in row["languages"].split(", ") if item]
        publisher = row["publisher"]
        seal = row["seal"] == "Yes"
        licence = row["licence"]
        apc = row["apc"] == "Yes"

        categories = self.getJournalCategories(set(ids))
        areas = self.getJournalAreas(set(ids))

        return Journal(ids, title, languages, publisher, seal, licence, apc, categories, areas)

    def getJournalCategories(self, journal_ids):
        all_dfs = [query.getJournalCategories(journal_ids) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        categories = []
        for index, row in merged_df.iterrows():
            category = Category(row["name"], row["quartile"])
            categories.append(category)

        return categories

    def getJournalAreas(self, journal_ids):
        all_dfs = [query.getJournalAreas(journal_ids) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        areas = []
        for index, row in merged_df.iterrows():
            area = Area(row["name"])
            areas.append(area)

        return areas

    def filterJournalsByIds(self, df, ids):
        merged_df = pd.DataFrame()
        for couple in ids:
            for id in couple:
                if id is None:
                    continue
                new_df = df[df["identifier"].str.contains(id, na=False)]
                merged_df = pd.concat([merged_df, new_df]).drop_duplicates().reset_index(drop=True)
                if not new_df.empty:
                    break

        return merged_df

class FullQueryEngine(BasicQueryEngine):
    def getJournalsInCategoriesWithQuartile(self, category_ids, quartiles):
        all_dfs = [query.getJournalsByCategoryWithQuartile(category_ids, quartiles) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        identifiers = merged_df.values.tolist()

        all_dfs = [query.getAllJournals() for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        filtered_df = self.filterJournalsByIds(merged_df, identifiers)

        journals = []
        for index, row in filtered_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getJournalsInAreasWithLicense(self, areas_ids, licenses):
        all_dfs = [query.getJournalsByArea(areas_ids) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        identifiers = merged_df.values.tolist()

        all_dfs = [query.getJournalsWithLicense() for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        filtered_df = self.filterJournalsByIds(merged_df, identifiers)

        journals = []
        for index, row in filtered_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals

    def getDiamondJournalsInAreasAndCategoriesWithQuartile(self, areas_ids, category_ids, quartiles):
        all_dfs = [query.getJournalsByAreaAndCategoryWithQuartile(areas_ids, category_ids, quartiles) for query in self.categoryQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        identifiers = merged_df.values.tolist()

        all_dfs = [query.getJournalsWithoutAPC() for query in self.journalQuery]
        merged_df = pd.concat(all_dfs).drop_duplicates().reset_index(drop=True) if all_dfs else pd.DataFrame()

        filtered_df = self.filterJournalsByIds(merged_df, identifiers)

        journals = []
        for index, row in filtered_df.iterrows():
            journal = self.buildJournal(row)
            journals.append(journal)

        return journals
