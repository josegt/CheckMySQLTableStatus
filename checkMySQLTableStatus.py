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

class Attribute:
    def __init__ (self, name, warningLimit = None, criticalLimit = None):
        self.__name = name
        self.__warningLimit = warningLimit
        self.__criticalLimit = criticalLimit

    def __str__ (self):
        return self.__name

    def getWarningLimit (self):
        return self.__warningLimit

    def getCriticalLimit (self):
        return self.__criticalLimit

class Table:
    def __init__ (self, schema, name):
        self.__schema = schema
        self.__name = name

    def __str__ (self):
        return self.__schema + '.' + self.__name

class Checker:
    class Check:
        def __init__ (self, table, attribute, value):
            self.__table = table
            self.__attribute = attribute
            self.__value = value

        def critical (self):
            '''Returns critical string if the limit exceeded.'''
            if self.__attribute.getCriticalLimit ():
                if self.__value > self.__attribute.getCriticalLimit ():
                    critical = str (self.__table) + '.' + str (self.__attribute)
                    critical += ' = ' + str (self.__value) + ' reached '
                    critical += str (self.__attribute.getCriticalLimit ())
                    return critical

        def warning (self):
            '''Returns warning string if the limit exceeded.'''
            if self.__attribute.getWarningLimit ():
                if self.__value > self.__attribute.getWarningLimit ():
                    warning = str (self.__table) + '.' + str (self.__attribute)
                    warning += ' = ' + str (self.__value) + ' reached '
                    warning += str (self.__attribute.getWarningLimit ())
                    return warning

        def posting (self):
            '''Returns performance data for Nagios.'''
            performanceData = str (self.__table) + '.' + str (self.__attribute)
            performanceData += '=' + str (int (self.__value)) + ';'
            if self.__attribute.getWarningLimit ():
                performanceData += str (int (self.__attribute.getWarningLimit ()))
            performanceData += ';'
            if self.__attribute.getCriticalLimit ():
                performanceData += str (int (self.__attribute.getCriticalLimit ()))
            performanceData += ';0;'
            return performanceData

    def __init__ (self):
        self.__checks = []

    def addCheck (self, *args):
        self.__checks.append (self.Check (*args))

    def check (self):
        '''Returns critical, warning strings and performance data for Nagios.
        Does not repeat criticals in warning.'''
        criticals = []
        warnings = []
        postings = []
        for check in self.__checks:
            critical = check.critical ()
            if critical:
                criticals.append (critical)
            else:
                warning = check.warning ()
                if warning:
                    warnings.append (warning)
            postings.append (check.posting ())

        return criticals, warnings, postings

class Readme:
    def __init__ (self):
        '''Parses texts under headers as sections on the readme file in the
        repository..'''
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

    def execute (self, query):
        self.__cursor.execute (query)
        return self.__cursor.fetchall ()

    def getColumnPosition (self, columnName):
        names = [desc [0] for desc in self.__cursor.description]
        for id, name in enumerate (names):
            if columnName.lower () == name.lower ():
                return id

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
                                 help = 'warning limits')
    argumentParser.add_argument ('-c', '--critical', type = __addOption, dest = 'criticalLimits',
                                 help = 'critical limits')

    return argumentParser.parse_args ()

if __name__ == '__main__':
    attributes = []
    arguments = parseArguments ()
    for counter, mode in enumerate (arguments.modes):
        warningLimit = None
        if arguments.warningLimits:
            if counter < len (arguments.warningLimits):
                warningLimit = Value (arguments.warningLimits[counter])
        criticalLimit = None
        if arguments.criticalLimits:
            if counter < len (arguments.criticalLimits):
                criticalLimit = Value (arguments.criticalLimits[counter])
        attributes. append (Attribute (mode, warningLimit, criticalLimit))

    import sys
    checker = Checker ()
    try:
        database = Database (arguments.host, arguments.port, arguments.user, arguments.passwd)
    except:
        print 'unknown: Cannot connected to the database.',
        sys.exit (3)

    for schemaRow in database.execute ('Show schemas'):
        showTablesQuery = 'Show table status in %s where Engine is not null' % schemaRow [0]
        for tableRow in database.execute (showTablesQuery):
            table = Table (schemaRow [0], tableRow [0])
            for attribute in attributes:
                columnPosition = database.getColumnPosition (str (attribute))
                if tableRow[columnPosition]:
                    checker.addCheck (table, attribute, Value (tableRow[columnPosition]))
    criticals, warnings, postings = checker.check ()

    print 'CheckMySQLTableStatus',
    if criticals:
        print 'critical:',
        for critical in criticals:
            print critical + ';',
    if warnings:
        print 'warning:',
        for warning in warnings:
            print warning + ';',
    if not criticals and not warnings:
        print 'ok',
    if postings:
        print '|',
        for posting in postings:
            print posting,
    if criticals:
        sys.exit (2)
    if warnings:
        sys.exit (1)
    sys.exit (0)
