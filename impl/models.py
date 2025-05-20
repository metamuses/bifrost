class IdentifiableEntity:
    def __init__(self, id):
        if isinstance(id, list):
            self.id = set(id)
        elif isinstance(id, str):
            self.id = set([id])

    def getIds(self):
        return list(self.id)

class Area(IdentifiableEntity):
    def __init__(self, id):
        super().__init__(id)

class Category(IdentifiableEntity):
    def __init__(self, id, quartile=None):
        super().__init__(id)
        self.quartile = quartile

    def getQuartile(self):
        return self.quartile

class Journal(IdentifiableEntity):
    def __init__(self, id, title="", languages=[], publisher=None, seal=False, licence="", apc=False, categories=[], areas=[]):
        super().__init__(id)
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.licence = licence
        self.apc = apc
        self.categories = categories
        self.areas = areas

    def getTitle(self):
        return self.title

    def getLanguages(self):
        return self.languages

    def getPublisher(self):
        return self.publisher

    def hasDOASeal(self):
        return self.seal

    def getLicence(self):
        return self.licence

    def hasAPC(self):
        return self.apc

    def getCategories(self):
        return self.categories

    def getAreas(self):
        return self.areas

    def hasCategory(self):
        if not self.categories:
            return False
        else:
            return True

    def hasArea(self):
        if not self.areas:
            return False
        else:
            return True
