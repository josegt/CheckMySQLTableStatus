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
        if str (value)[-1:] in ('K', 'M', 'G'):
            self.__int = int (value[:-1])
            self.__unit = value[-1:]
        else:
            self.__int = int (value)
            self.__unit = None

    def __str__ (self):
        if self.__unit:
            return str (self.__int) + self.__unit
        if self.__int > 10 ** 9:
            return str (round (self.__int / 10 ** 9))[:-2] + 'G'
        if self.__int > 10 ** 6:
            return str (round (self.__int / 10 ** 6))[:-2] + 'M'
        if self.__int > 10 ** 3:
            return str (round (self.__int / 10 ** 3))[:-2] + 'K'
        return str (self.__int)

    def __int__ (self):
        if self.__unit == 'K':
            return self.__int * 10 ** 3
        if self.__unit == 'M':
            return self.__int * 10 ** 6
        if self.__unit == 'G':
            return self.__int * 10 ** 9
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
            if self.__attribute.getCriticalLimit ():
                if self.__value > self.__attribute.getCriticalLimit ():
                    critical = str (self.__table) + '.' + str (self.__attribute)
                    critical += ' = ' + str (self.__value) + ' reached '
                    critical += str (self.__attribute.getCriticalLimit ())
                    return critical

        def warning (self):
            if self.__attribute.getWarningLimit ():
                if self.__value > self.__attribute.getWarningLimit ():
                    warning = str (self.__table) + '.' + str (self.__attribute)
                    warning += ' = ' + str (self.__value) + ' reached '
                    warning += str (self.__attribute.getWarningLimit ())
                    return warning

        def posting (self):
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
    __lines = []
    def __init__ (self):
        readmeFile = open ('README.md')
        self.__lines.append (readmeFile.readline ())
        readmeFile.close ()

    def title (self):
        return self.__lines[0]

def parseOptions ():
    readme = Readme ()
    from optparse import OptionParser
    optionParser = OptionParser (description = readme.title ())
    optionParser.add_option ('-H', '--host', action = 'store', type = 'string',
                             dest = 'host', default = 'localhost',
                             help = 'hostname')
    optionParser.add_option ('-P', '--port', action = 'store', type = 'int',
                             dest = 'port', default = '3306', help = 'port')
    optionParser.add_option ('-u', '--user', action = 'store', type = 'string',
                             dest = 'user', help = 'username')
    optionParser.add_option ('-p', '--pass', action = 'store', type = 'string',
                             dest = 'passwd', help = 'password')
    return optionParser.parse_args ()

if __name__ == '__main__':
    attributes = {}
    attributes[1] = Attribute ('rowCount', warningLimit = Value ('100M'))
    attributes[2] = Attribute ('dataLength', warningLimit = Value ('50G'))
    attributes[3] = Attribute ('indexLength', warningLimit = Value ('50G'))
    attributes[4] = Attribute ('dataFree', warningLimit = Value ('500M'))
    attributes[5] = Attribute ('autoIncrement', warningLimit = Value ('2G'))

    checker = Checker ()
    options, arguments = parseOptions ()
    import MySQLdb
    connection = MySQLdb.connect (host = options.host, port = options.port,
                                  user = options.user, passwd = options.passwd)
    cursor = connection.cursor ()
    cursor.execute ('Show schemas')
    for schemaRow in cursor.fetchall ():
        showTablesQuery = 'Show table status in {} where Engine is not null'
        cursor.execute (showTablesQuery.format (schemaRow[0]))
        for tableRow in cursor.fetchall ():
            table = Table (schemaRow[0], tableRow[0])
            if tableRow[4]:
                checker.addCheck (table, attributes[1], Value (tableRow[4]))
            if tableRow[6]:
                checker.addCheck (table, attributes[2], Value (tableRow[6]))
            if tableRow[8]:
                checker.addCheck (table, attributes[3], Value (tableRow[8]))
            if tableRow[9]:
                checker.addCheck (table, attributes[4], Value (tableRow[9]))
            if tableRow[10]:
                checker.addCheck (table, attributes[5], Value (tableRow[10]))
    cursor.close ()
    connection.close ()
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

    import sys
    if criticals:
        sys.exit (2)
    if warnings:
        sys.exit (1)
    sys.exit (0)
