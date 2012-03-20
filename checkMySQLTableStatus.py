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

    def getPerformanceData (self, name, value):
        message = name + '.' + self._attribute + '=' + str (value) + ';'
        if self._warningLimit:
            message += str (int (self._warningLimit))
        message += ';'
        if self._criticalLimit:
            message += str (int (self._criticalLimit))
        return message + ';0;'

class OutputAll (Output):
    def __init__ (self, *args):
        Output.__init__ (self, *args)
        self.__message = ''

    def addMessageForTable (self, table):
        if self.__message:
            self.__message += ' '
        self.__message += self.getPerformanceData (str (table), int (table.getAttribute (self._attribute)))

    def check (self, table):
        if table.getAttribute (self._attribute):
            self.addMessageForTable (table)

    def getMessage (self, name):
        if name == 'performance':
            return self.__message

class OutputTables (OutputAll):
    def __init__ (self, tableNames, *args):
        OutputAll.__init__ (self, *args)
        self.__tableNames = tableNames

    def check (self, table):
        if table.getAttribute (self._attribute):
            for tableName in self.__tableNames:
                if tableName == str (table):
                    self.addMessageForTable (table)

class OutputUpperLimit (Output):
    def __init__ (self, *args):
        Output.__init__ (self, *args)
        self.__messages = {}

    def addMessageForTable (self, name, table, limit):
        if not self.__messages.has_key (name):
            self.__messages [name] = ''
        else:
            self.__messages [name] += ' '
        self.__messages [name] += str (table) + '.' + self._attribute + ' = '
        self.__messages [name] += str (table.getAttribute (self._attribute)) + ' reached '
        self.__messages [name] += str (limit) + ';'

    def check (self, table):
        '''Check for warning and critical limits. Do not add message to both warning and critical lists.'''
        if table.getAttribute (self._attribute):
            if self._criticalLimit and table.getAttribute (self._attribute) > self._criticalLimit:
                self.addMessageForTable ('critical', table, self._criticalLimit)
            elif self._warningLimit and table.getAttribute (self._attribute) > self._warningLimit:
                self.addMessageForTable ('warning', table, self._warningLimit)

    def getMessage (self, name):
        if self.__messages.has_key (name):
            return self.__messages [name]

class OutputAverage (Output):
    def __init__ (self, *args):
        Output.__init__ (self, *args)
        self.__count = 0
        self.__total = 0

    def check (self, table):
        '''Count tables and sum values for average calculation.'''
        if table.getAttribute (self._attribute):
            self.__count += 1
            self.__total += int (table.getAttribute (self._attribute))

    def getValue (self):
        return Value (round (self.__total / self.__count))

    def getMessage (self, name):
        if self.__count:
            if name == 'ok':
                return 'average ' + self._attribute + ' = ' + str (self.getValue ()) + ';'
            if name == 'performance':
                return self.getPerformanceData ('average', int (self.getValue ()))

class OutputMaximum (Output):
    def __init__ (self, *args):
        Output.__init__ (self, *args)
        self.__table = None

    def check (self, table):
        '''Get table which has maximum value.'''
        if table.getAttribute (self._attribute):
            if not self.__table or table.getAttribute (self._attribute) > self.__table.getAttribute (self._attribute):
                self.__table = table

    def getMessage (self, name):
        if self.__table:
            if name == 'ok':
                message = 'maximum ' + self._attribute + ' = ' + str (self.__table.getAttribute (self._attribute))
                return message + ' for table ' + str (self.__table) + ';'
            if name == 'performance':
                return self.getPerformanceData ('maximum', int (self.__table.getAttribute (self._attribute)))

class OutputMinimum (Output):
    def __init__ (self, *args):
        Output.__init__ (self, *args)
        self.__table = None

    def check (self, table):
        '''Get table which has minimum value.'''
        if table.getAttribute (self._attribute):
            if not self.__table or table.getAttribute (self._attribute) < self.__table.getAttribute (self._attribute):
                self.__table = table

    def getMessage (self, name):
        if self.__table:
            if name == 'ok':
                message = 'minimum ' + self._attribute + ' = ' + str (self.__table.getAttribute (self._attribute))
                return message + ' for table ' + str (self.__table) + ';'
            if name == 'performance':
                return self.getPerformanceData ('minimum', int (self.__table.getAttribute (self._attribute)))

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
                newMessage = output.getMessage (name)
                if newMessage:
                    if message:
                        message += ' '
                    message += newMessage
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
    defaultModes = 'rows,data_length,index_length'
    try:
        readme = Readme ()
        epilog = readme.getSectionsConcatenated ()
    except IOError:
        epilog = None
    from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentDefaultsHelpFormatter
    class HelpFormatter (RawTextHelpFormatter, ArgumentDefaultsHelpFormatter): pass
    def options (value):
        return value.split (',')

    argumentParser = ArgumentParser (formatter_class = HelpFormatter, description = description, epilog = epilog)
    argumentParser.add_argument ('-H', '--host', dest = 'host', help = 'hostname', default = 'localhost')
    argumentParser.add_argument ('-P', '--port', type = int, dest = 'port', help = 'port', default = 3306)
    argumentParser.add_argument ('-u', '--user', dest = 'user', required = True, help = 'username')
    argumentParser.add_argument ('-p', '--pass', dest = 'passwd', required = True, help = 'password')
    argumentParser.add_argument ('-m', '--mode', type = options, dest = 'modes', help = 'modes', default = defaultModes)
    argumentParser.add_argument ('-w', '--warning', type = options, dest = 'warnings', help = 'warning limits')
    argumentParser.add_argument ('-c', '--critical', type = options, dest = 'criticals', help = 'critical limits')
    argumentParser.add_argument ('-t', '--tables', type = options, dest = 'tables', help = 'show selected tables')
    argumentParser.add_argument ('-a', '--all', dest = 'all', action = 'store_true', help = 'show all tables')
    argumentParser.add_argument ('-A', '--average', dest = 'average', action = 'store_true', help = 'show averages')
    argumentParser.add_argument ('-M', '--maximum', dest = 'maximum', action = 'store_true', help = 'show maximums')
    argumentParser.add_argument ('-N', '--minimum', dest = 'minimum', action = 'store_true', help = 'show minimums')
    return argumentParser.parse_args ()

if __name__ == '__main__':
    print 'CheckMySQLTableStatus',
    import sys
    try:
        arguments = parseArguments ()
        attributes = []
        checker = Checker ()
        for counter, mode in enumerate (arguments.modes):
            attributes.append (mode)
            warningLimit = None
            if arguments.warnings:
                if counter < len (arguments.warnings):
                    warningLimit = Value (arguments.warnings [counter])
            criticalLimit = None
            if arguments.criticals:
                if counter < len (arguments.criticals):
                    criticalLimit = Value (arguments.criticals [counter])
            checker.addOutput (OutputUpperLimit (mode, warningLimit, criticalLimit))
            if arguments.all:
                checker.addOutput (OutputAll (mode, warningLimit, criticalLimit))
            elif arguments.tables:
                checker.addOutput (OutputTables (arguments.tables, mode, warningLimit, criticalLimit))
            if arguments.average:
                checker.addOutput (OutputAverage (mode, warningLimit, criticalLimit))
            if arguments.maximum:
                checker.addOutput (OutputMaximum (mode, warningLimit, criticalLimit))
            if arguments.minimum:
                checker.addOutput (OutputMinimum (mode, warningLimit, criticalLimit))


        database = Database (arguments.host, arguments.port, arguments.user, arguments.passwd)
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
    except Exception as exception:
        print 'unknown:', exception
        sys.exit (3)
