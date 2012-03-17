#!/usr/bin/env python
##
# Tart Database Operations
# Check MySQL Table Status
#
# @author  Emre Hasegeli <emre.hasegeli@tart.com.tr>
# @date    2012-01-31
##

class Value:
    def __init__ (self, value):
        '''Parses the value.'''
        if str (value) [-1:] in ('K', 'M', 'G', 'T'):
            self.__int = int (value [:-1])
            self.__unit = value [-1:]
        else:
            self.__int = int (value)
            self.__unit = None

    def __str__ (self):
        '''If necessary changes the value to number + unit format by rounding.'''
        if self.__unit:
            return str (self.__int) + self.__unit
        if self.__int > 10 ** 12:
            return str (round (self.__int / 10 ** 12)) [:-2] + 'T'
        if self.__int > 10 ** 9:
            return str (round (self.__int / 10 ** 9)) [:-2] + 'G'
        if self.__int > 10 ** 6:
            return str (round (self.__int / 10 ** 6)) [:-2] + 'M'
        if self.__int > 10 ** 3:
            return str (round (self.__int / 10 ** 3)) [:-2] + 'K'
        return str (self.__int)

    def __int__ (self):
        '''If necessary changes the value to number format.'''
        if self.__unit == 'K':
            return self.__int * 10 ** 3
        if self.__unit == 'M':
            return self.__int * 10 ** 6
        if self.__unit == 'G':
            return self.__int * 10 ** 9
        if self.__unit == 'T':
            return self.__int * 10 ** 12
        return self.__int

    def __cmp__ (self, other):
        return cmp (int (self), int (other))

class Table:
    def __init__ (self, schema, name):
        self.__schema = schema
        self.__name = name
        self.__attributes = {}

    def __str__ (self):
        return self.__schema + '.' + self.__name

    def addAttribute (self, name, value):
        self.__attributes [name] = value

    def getAttribute (self, name):
        if self.__attributes.has_key (name):
            return self.__attributes [name]

class Output:
    def __init__ (self, attribute, warningLimit = None, criticalLimit = None):
        self._attribute = attribute
        self._warningLimit = warningLimit
        self._criticalLimit = criticalLimit

    def performanceDataSuffix (self):
        return ';' + str (self._warningLimit or '') + ';' + str (self._criticalLimit or '') + ';0; '

class OutputAll (Output):
    def __init__ (self, *args):
        Output.__init__ (self, *args)
        self.__tables = []

    def check (self, table):
        if table.getAttribute (self._attribute):
            self.__tables.append (table)

    def getMessage (self, name):
        if name == 'performance':
            message = ''
            for table in self.__tables:
                message += str (table) + '.' + self._attribute + '=' + str (int (table.getAttribute (self._attribute)))
                message += self.performanceDataSuffix ()
            return message
        return ''

class OutputUpperLimit (Output):
    def __init__ (self, *args):
        Output.__init__ (self, *args)
        self.__warningTables = []
        self.__criticalTables = []

    def check (self, table):
        '''Check for warning and critical limits. Do not add table to both warning and critical lists.'''
        if table.getAttribute (self._attribute):
            if self._criticalLimit and table.getAttribute (self._attribute) > self._criticalLimit:
                self.__criticalTables.append (table)
            elif self._warningLimit and table.getAttribute (self._attribute) > self._warningLimit:
                self.__warningTables.append (table)

    def getTableMessagesConcatenated (self, tables, limit):
        message = ''
        for table in tables:
            message += str (table) + '.' + self._attribute + ' = ' + str (table.getAttribute (self._attribute))
            message += ' reached ' + str (limit) + '; '
        return message

    def getMessage (self, name):
        if name == 'warning':
            return self.getTableMessagesConcatenated (self.__warningTables, self._warningLimit)
        if name == 'critical':
            return self.getTableMessagesConcatenated (self.__criticalTables, self._criticalLimit)
        return ''

class OutputAggeregate (Output):
    def getMessage (self, name):
        if self.getValue ():
            if name == 'ok':
                return self.getParameter () + ' = ' + str (self.getValue ()) + '; '
            if name == 'performance':
                return self.getParameter () + '=' + str (int (self.getValue ())) + self.performanceDataSuffix ()
        return ''

class OutputAverage (OutputAggeregate):
    def __init__ (self, *args):
        Output.__init__ (self, *args)
        self.__count = 0
        self.__total = 0

    def check (self, table):
        '''Count tables and sum values for average calculation.'''
        if table.getAttribute (self._attribute):
            self.__count += 1
            self.__total += int (table.getAttribute (self._attribute))

    def getParameter (self):
        return self._attribute + '.average'

    def getValue (self):
        if self.__count:
            return Value (self.__total / self.__count)

class OutputMaximum (OutputAggeregate):
    def __init__ (self, *args):
        Output.__init__ (self, *args)
        self.__table = None

    def check (self, table):
        '''Get table which has maximum value.'''
        if table.getAttribute (self._attribute):
            if not self.__table or table.getAttribute (self._attribute) > self.__table.getAttribute (self._attribute):
                self.__table = table

    def getParameter (self):
        return self._attribute + '.maximum'

    def getValue (self):
        if self.__table:
            return self.__table.getAttribute (self._attribute)

class OutputMinimum (OutputAggeregate):
    def __init__ (self, *args):
        Output.__init__ (self, *args)
        self.__table = None

    def check (self, table):
        '''Get table which has minimum value.'''
        if table.getAttribute (self._attribute):
            if not self.__table or table.getAttribute (self._attribute) < self.__table.getAttribute (self._attribute):
                self.__table = table

    def getParameter (self):
        return self._attribute + '.minimum'

    def getValue (self):
        if self.__table:
            return self.__table.getAttribute (self._attribute)

class Checker:
    def __init__ (self):
        self.__outputs = []

    def addOutput (self, output):
        assert isinstance (output, Output)
        self.__outputs.append (output)

    def check (self, *args):
        for output in self.__outputs:
            if hasattr (output, 'check'):
                output.check (*args)

    def getMessage (self, name):
        message = ''
        for output in self.__outputs:
            if hasattr (output, 'getMessage'):
                message += output.getMessage (name)
        return message

class Readme:
    def __init__ (self):
        '''Parse texts on the readme file on the repository to sections..'''
        readmeFile = open ('README.md')
        self.__sections = []
        for line in readmeFile.readlines ():
            if line [:2] == '##':
                self.__sections.append (line [3:-1] + ':\n')
            elif self.__sections and line [:-1] not in ('```', ''):
                self.__sections [-1] += line
        readmeFile.close ()

    def getSectionsConcatenated (self):
        body = ''
        for section in self.__sections:
            body += section + '\n'
        return body

class Database:
    __cursor = None
    def __init__ (self, host, port, user, passwd):
        import MySQLdb
        self.__connection = MySQLdb.connect (host = host, port = port, user = user, passwd = passwd)
        self.__cursor = self.__connection.cursor ()

    def __del__ (self):
        if self.__cursor:
            self.__cursor.close ()
            self.__connection.close ()

    def select (self, query):
        self.__cursor.execute (query)
        return self.__cursor.fetchall ()

    def getColumnPosition (self, columnName):
        names = [desc [0] for desc in self.__cursor.description]
        for __id, name in enumerate (names):
            if columnName.lower () == name.lower ():
                return __id

def parseArguments ():
    description = 'Multiple vales can be given comma separated to modes and limits.\n'
    description += 'K for 10**3, M for 10**6, G for 10**9, T for 10**12 units can be used for limits.\n'
    try:
        readme = Readme ()
        epilog = readme.getSectionsConcatenated ()
    except IOError:
        epilog = None
    from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentDefaultsHelpFormatter
    class HelpFormatter (RawTextHelpFormatter, ArgumentDefaultsHelpFormatter): pass
    argumentParser = ArgumentParser (formatter_class = HelpFormatter,
                                     description = description, epilog = epilog)

    argumentParser.add_argument ('-H', '--host', dest = 'host', default = 'localhost',
                                 help = 'hostname')
    argumentParser.add_argument ('-P', '--port', type = int, dest = 'port', default = 3306,
                                 help = 'port')
    argumentParser.add_argument ('-u', '--user', dest = 'user', required = True,
                                 help = 'username')
    argumentParser.add_argument ('-p', '--pass', dest = 'passwd', required = True,
                                 help = 'password')

    def __addOption (value):
        return value.split (',')
    argumentParser.add_argument ('-m', '--mode', type = __addOption, dest = 'modes',
                                 default = 'rows,data_length,index_length', help = 'modes')
    argumentParser.add_argument ('-w', '--warning', type = __addOption, dest = 'warningLimits',
                                 help = 'warning upper limits')
    argumentParser.add_argument ('-c', '--critical', type = __addOption, dest = 'criticalLimits',
                                 help = 'critical upper limits')
    argumentParser.add_argument ('-A', '--all', dest = 'all', action = 'store_true',
                                 default = False, help = 'show all values as performance data')
    argumentParser.add_argument ('-B', '--average', dest = 'average', action = 'store_true',
                                 default = False, help = 'show averages')
    argumentParser.add_argument ('-M', '--maximum', dest = 'maximum', action = 'store_true',
                                 default = False, help = 'show maximums')
    argumentParser.add_argument ('-N', '--minimum', dest = 'minimum', action = 'store_true',
                                 default = False, help = 'show minimums')

    return argumentParser.parse_args ()

if __name__ == '__main__':
    print 'CheckMySQLTableStatus',

    arguments = parseArguments ()
    attributes = []
    checker = Checker ()
    for counter, mode in enumerate (arguments.modes):
        attributes.append (mode)
        warningLimit = None
        if arguments.warningLimits:
            if counter < len (arguments.warningLimits):
                warningLimit = Value (arguments.warningLimits [counter])
        criticalLimit = None
        if arguments.criticalLimits:
            if counter < len (arguments.criticalLimits):
                criticalLimit = Value (arguments.criticalLimits [counter])
        checker.addOutput (OutputUpperLimit (mode, warningLimit, criticalLimit))
        if arguments.all:
            checker.addOutput (OutputAll (mode, warningLimit, criticalLimit))
        if arguments.average:
            checker.addOutput (OutputAverage (mode, warningLimit, criticalLimit))
        if arguments.maximum:
            checker.addOutput (OutputMaximum (mode, warningLimit, criticalLimit))
        if arguments.minimum:
            checker.addOutput (OutputMinimum (mode, warningLimit, criticalLimit))

    import sys
    try:
        database = Database (arguments.host, arguments.port, arguments.user, arguments.passwd)
    except:
        print 'unknown: Cannot connect to the database.',
        sys.exit (3)

    for schemaRow in database.select ('Show schemas'):
        showTablesQuery = 'Show table status in %s where Engine is not null' % schemaRow [0]
        for tableRow in database.select (showTablesQuery):
            table = Table (schemaRow [0], tableRow [0])
            for attribute in attributes:
                columnPosition = database.getColumnPosition (attribute)
                if tableRow[columnPosition]:
                    table.addAttribute (attribute, Value (tableRow[columnPosition]))
            checker.check (table)

    criticalMessage = checker.getMessage ('critical')
    if criticalMessage:
        print 'critical:', criticalMessage,
    warningMessage = checker.getMessage ('warning')
    if warningMessage:
        print 'warning:', warningMessage,
    if not criticalMessage and not warningMessage:
        okMessage = checker.getMessage ('ok')
        if okMessage:
            print 'ok:', okMessage,
        else:
            print 'ok',
    performanceData = checker.getMessage ('performance')
    if performanceData:
        print '|', performanceData,

    if criticalMessage:
        sys.exit (2)
    if warningMessage:
        sys.exit (1)
    sys.exit (0)
