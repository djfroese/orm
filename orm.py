import datetime, hmac, datastore, json, web
    

class Property(object):
    """Top Level Class that describes a generic property that stores a value.
    It has three methods, a getter and a setter for the value, as well as a method return a new object of
    type Property"""
    
    _default = None
    
    sizes = {'tiny':1, 'small':2, 'medium':4, 'large':8, 'xlarge':16 }
    sizeFactor = 16
    
    def __repr__(self):
        return self._val.__repr__()
    
    def __init__(self,PrimaryKey=False, size='large'):
        """Initiates the property and sets whether this is the primary key property or not."""
        self._primarykey = PrimaryKey
        self._val = self._default
        self._size = size
    
    
    def getval(self):
        """Returns the value of _val"""
        return self._val
    
    def setval(self, value):
        """Sets the value of _val"""
        self._val = value
    
    def jsonval(self):
        """Returns the value in json serializable format"""
        return self._val
    
    def adhoc(self):
        """Returns a new object of the same type as the calling object."""
        k = self.__class__
        return k()


class IntegerProperty(Property):
    """A subclass of property that holds an Integer value instead of a generic value."""
    _default = 0
    
    def setval(self, value):
        #value = int(value)
        if isinstance(value,int) or isinstance(value,long) or value == None:
            self._val = value
        else:
            raise TypeError
    

class StringProperty(Property):
    """A subclass of property that holds an String value instead of a generic value."""
    
    _default = ''
    
    def setval(self, value):
        #print type(value)
        if value is None:
            value = self._default
        
        if isinstance(value,str) or isinstance(value,unicode):
            self._val = str(value)
        else:
            raise TypeError, value.__class__

class ListProperty(Property):
    """A subclass of property that holds a list of string or integer values."""
    
    _default = []
    
    def setval(self, value):
        if value is None:
            value = self._default
        
        if isinstance(value,list):
            self._val = value
        else:
            raise TypeError
    
    def getval(self):
        if self._val is None:
            self._val = _default
        
        return self._val

class DateTimeProperty(Property):
    """A subclass of property that holds an datetime value instead of a generic value."""
    
    _default = datetime.datetime.now
    
    def getval(self):
        if self._val is None:
            self._val = self._default()
        
        return self._val
    
    def setval(self, value):
        if value is None or value == '':
            self._val = value
        else:
            if isinstance(value, str) or isinstance(value,unicode):
                try:
                    value = datetime.datetime.strptime(value,"%Y-%m-%d %H:%M:%S")
                except ValueError:
                    try:
                        value = datetime.datetime.strptime(value,"%Y-%m-%d")
                    except ValueError:
                        raise TypeError
            
            if isinstance(value,datetime.datetime):
                self._val = value
            else:
                raise TypeError

    def jsonval(self):
        return self._val.strftime("%Y-%m-%d %H:%M:%S")

class ModelQuery(object):
    
    def __init__(self, cls):
        self._filters = []
        self._order = ''
        self._tablename = cls.__name__
        self._class = cls
        self._limit = 100
    
    def filter(self, property_op, value):
        """Adds a filter which will be turned into a WHERE statement"""
        if property_op.endswith('in', 0, len(property_op)):
            if isinstance(value, list) and len(value) > 0:
                self._filters.append("%s (%s)"%(property_op,web.db.sqllist(value)))
        else:
            self._filters.append("%s '%s'"%(property_op,value))
        return self
    
    def order(self, column):
        """Sets order to return objects in."""
        if column[0] == '-':
            self._order = '%s DESC'%column[1:]
        else:
            self._order = '%s ASC'%column
        return self
        
    def query(self):
        """Returns the Query or SQL command to be executed."""
        return ""
    
    def execute(self, single=False, combo=False):
        """Executes query and returns results based upon set filters and order methods"""
        if combo:
            _where = ' or '.join(self._filters)
        else:
            _where = ' and '.join(self._filters)
        
        if not _where:
            _where = None
        if self._order == '':
            _order = None
        else:
            _order = self._order
        
        values = datastore.select(self._tablename,where=_where,order=_order,limit=self._limit)
        if values:
            values = [self._class.make(x) for x in values]
        
        # returns object if only one item found
        if len(values) == 1 and single:
            values = values[0]
    
        return values
    
    def limit(self, count):
        if isinstance(count, int) and count > 0 and count < 500:
            self._limit = count

class Model(object):
    """Model object is the top level class upon which all Model Classes inherit."""
    processed = False
    primarykeys = []
    
    def __repr__(self):
        return self.properties.__repr__()
    
    def getval(self, n):
        """Returns the value of object named <n> from the properties dictionary."""
        return self.properties[n].getval()
    
    def setval(self, n, value):
        """Sets the value of object named <n> from the properties dictionary."""
        self.properties[n].setval(value)
    
    def __repr__(self):
        result = "ORM %s Model Object: %s"%(self.__class__.__name__,self.properties.__repr__())
        return result
    
    def __new__(cls, *args, **kwargs):
        """Overloads the __new__ function to updated the class with
        properties defined in a subclass of Model."""        
        obj = super(type(cls), cls).__new__(cls)
        
        # checks whether or not the properties defined in the subclass have been created yet.
        cls._tablename = cls.__name__
        if not cls.processed:
            cls.primarykeys = []
            # for all objects in __dict__ if it is an instance of Property (defined in subclass)
            # creates a property (getters, setters) for the named property, and hides the original
            # Property Objects.
            cls._tablename = cls.__name__
            for k,v in cls.__dict__.items():
                if isinstance(v,Property):
                    if v._primarykey:
                        cls.primarykeys.append(k)
                    if k[0] == "_":
                        n = k[1:]
                    else:
                        #print "First Time Creating object saves class objects to _%s."%k
                        n = k
                        h = "_%s"%k
                        setattr(cls,h,v)
                        # generates the functions to call for getter/setter as we need more information
                        # than the basic getter/setter provides we pass these to make custom functions.
                        setattr(cls,n,property(obj.make_getter(n),obj.make_setter(n)))
                       
            cls.processed = True
        
        return obj
        
    
    def __init__(self):
        """Get tablename to match Class name and creates properties dictionary to hold properties."""
        self.__tablename__ = self.__class__.__name__
        cls = self.__class__
        if hasattr(cls,'primarykeys'):
            self._primarykeys = cls.primarykeys
        else:
            self._primarykeys = []
            
        self.properties = { k[1:]: v.adhoc() for k,v in cls.__dict__.items() if isinstance(v,Property) }
        
        
    def make_getter(self, n):
        """Make and return a getter function."""
        return lambda self: self.getval(n)
    
    def make_setter(self, n):
        """Make and return a setter function."""
        return lambda self, value: self.setval(n,value)

    def display(self):
        """Displays property value pairs one to a row."""
        for k,v in self.properties.items():
            print '%s : %s'%(k,v.getval())
        print
    
    
    @classmethod
    def makekey(cls,value):
        "Make a key value for the cache store."
        secret = 'jfkl;aNNdip[10[98dfnl;a&fnfnip-1nvmcal;f8bjgls8dg]99anc]'
        newkey = hmac.new(secret, value).hexdigest()
        return newkey
    
    @classmethod
    def make(cls,values):
        """Returns an object of type Model or subclass of Model and sets the properties from values."""
        result = cls()
        for p,v in result.properties.items():
            #print type(value[p]), p
            v.setval(values[p])
            #result.__setattr__(p,value[p])
        return result
    
    @classmethod
    def table(cls):
        return cls.__name__
    
    @classmethod
    def key(cls, key=None,**keys):
        """Takes either a dictionary or a single key. Returns single object or None if keys didn't match any object."""
        result = cls()
        value = None
        
        if result._primarykeys == []:
            raise KeyError
        
        if key and len(result._primarykeys) == 1:
            _where = {result._primarykeys[0]: key}
        else:
            _where = keys if keys.keys() == sorted(result._primarykeys) else None

        if _where is not None:
            value = datastore.select(result.__tablename__,where=web.db.sqlwhere(_where)).list()
        
        if value and len(value) == 1: 
            return cls.make(value[0])
        
        return None

    @classmethod
    def all(cls):
        """Returns a Model Query object for the calling class."""
        q = ModelQuery(cls)
        return q
    
    @classmethod
    def load(cls,value):
        """Returns an object from a given json representation of the object."""
        if not value:
            return None
        ps = json.loads(value)
        return cls.make(ps)
        
    def dump(self):
        """Returns a json string representation of the object."""
        ps = { k:v.jsonval() for k,v in self.properties.items() }
        return json.dumps(ps)
            
    def put(self):
        """Updates or Inserts the object into the database depending if it already exists or not.
        This is the one place where caching should absolutely NOT be done."""
        props = { k:v.getval() for k,v in self.properties.items() }
        
        where_key = {k:self.getval(k) for k in self._primarykeys}
        if self.__class__.key(**where_key):
            datastore.update(self.__tablename__,where=web.db.sqlwhere(where_key),**props)
        else:
            for key in self._primarykeys:
                del props[key]
            rowid = datastore.insert(self.__tablename__,**props)
            self.setval(self._primarykeys[0],rowid)
        ## return needs to handle no primary key, one primary key, multiple primary keys
        return self

    def remove(self):
        """Removes the current object from the datastore."""
        where_key = {k:self.getval(k) for k in self._primarykeys}
        datastore.delete(self.__tablename__,where=web.db.sqlwhere(where_key))
    
    
