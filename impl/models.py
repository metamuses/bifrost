class IdentifiableEntity:
    def __init__(self, id):
        self.id = id

    def getIds(self):
        # TODO: Implement this method
        pass

class Area(IdentifiableEntity):
    # TODO: Implement this class
    pass

class Category(IdentifiableEntity):
    def __init__(self, id, quartile=None):
        super().__init__(id)
        self.quartile = quartile

    def getQuartile(self):
        return self.quartile

class Journal(IdentifiableEntity):
    def __init__(self, id, title, languages, publisher=None, seal=False, licence="", apc=False):
        super().__init__(id)
        self.title = title
        self.languages = languages
        self.publisher = publisher
        self.seal = seal
        self.licence = licence
        self.apc = apc

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
        return []

    def getAreas(self):
        return []
