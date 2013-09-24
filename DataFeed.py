
import csv, urllib2, time, pdb
import Sponta
from Schema import Database
from Sponta import logMessage

Symbol          = "symbol"
Feed            = "feed"
Last            = "last_price"
LastTime        = "last_time"
LastDate        = "last_date"
LastTradeDate   = "last_trade_date" # combine LastTime and LastDate
Change          = "change"
Open            = "open_price"
PrevClose       = "prev_close"
DayLow          = "day_low"
DayHigh         = "day_high"
Volume          = "volume"
AvgVolume       = "avg_volume"
Low52           = "low52"
High52          = "high52"
PriceEarning    = "pe"
PriceSales      = "pr_sales"
PriceBook       = "pr_book"
Dividend        = "dividend"
Yield           = "yield"
MarketCap       = "mkt_cap"
Timestamp       = "timestamp"

# Mapping of market_quote table column value types
TypesMapping = {
    Symbol          : "s",
    Feed            : "s",
    Last            : "f",
    LastTradeDate   : "i",
    Change          : "f",
    Open            : "f",
    PrevClose       : "f",
    DayLow          : "f",
    DayHigh         : "f",
    Volume          : "i",
    AvgVolume       : "i",
    Low52           : "f",
    High52          : "f",
    Dividend        : "f",
    Yield           : "f",
    MarketCap       : "s",
    Timestamp       : "i",
    }

# Quote fields, for now a subset of market_quote columns
QuoteFields = [ Symbol, Feed, Last, LastTradeDate, Open, PrevClose, DayHigh, DayLow,
                Volume, AvgVolume, High52, Low52, Timestamp ]

# The set of fields we fetch for market quote. The fields of quotes we get are
# in this order.
#
YahooQuoteMapping = [
    ( Symbol          , "s"  ),
    ( LastDate        , "d1" ),    
    ( LastTime        , "t1" ),
    ( Last            , "l1" ),    
    ( Change          , "c1" ),
    ( Open            , "o"  ),
    ( PrevClose       , "p"  ),
    ( DayLow          , "g"  ),
    ( DayHigh         , "h"  ),
    ( Volume          , "v"  ),
    ( AvgVolume       , "a2" ),
    ( Low52           , "j"  ),
    ( High52          , "k"  ),
    ( Dividend        , "d"  ),
    ( Yield           , "y"  ),
    ( MarketCap       , "j1" )
    ]

YahooFieldIndex = {}
for i, f in enumerate( YahooQuoteMapping ):
    YahooFieldIndex[ f[0] ] = i

class Yahoo:
    '''
    Interface to Yahoo data source. It provides three functions:

    1) Current market quote. It might be all zero if the market is not open.
    2) Historical prices. This is the same as Yahoo historical price page.
    3) Symbol fundamentals.

    In the future we should provide more Yahoo data via this class, such as
    4) Company events
    5) Insider transaction
    etc.
    
    '''
    def __init__( self ):

        pass

    def openCsvUrl( self, url ):
        '''
        Open a URL that returns a CSV response.

        @return a CVS reader object, or None if failed to fetch.
        '''
        request = urllib2.Request( url )
        request.add_header( "User-Agent", Sponta.UserAgents[ "Firefox16" ] )
        try :
            before = time.time()
            result = urllib2.urlopen( request )
            # may get csv error???
            reader = csv.reader( result )
            after = time.time()
            logMessage( "URL fetch time: %s" % ( after - before ) )
            return reader
        except urllib2.HTTPError as ex:
            print ex.code, ':', ex.reason
            self.error_ = ex
        return None

    def getHistoricalPrices( self, symbol, startDate, endDate=None,
                             period='d' ):
        '''
        Get Yahoo historical prices of one symbol. Default period is daily.
        
        @return a list of quotes. Each item in the list is a tuple of
                ( timestamp, open, high, low, close, volume, adj close )
                None if failed to fetch.
        '''
        
        if endDate is None:
            endDate = datetime.date.today
            
        url = "http://ichart.finance.yahoo.com/table.csv?" +  \
              "s=%s&a=%s&b=%s&c=%s&d=%s&e=%s&f=%s&g=%s" % (
            symbol, startDate.month - 1, startDate.day, startDate.year,
            endDate.month - 1, endDate.day, endDate.year, period )

        reader = self.openCsvUrl( url )
        if reader is None:
            logMessage( "Fetch failed for %s, start %s, end %s, " +
                        "period %s: %s" % (
                    symbol, startDate, endDate, period, self.error_ ) )
            return None
        
        data = []
        count = 0
        for row in reader:
            if count == 0:
                # First row of Yahoo csv response is column names
                assert row[0] == 'Date' and row[1] == 'Open' and \
                       row[2] == 'High' and row[3] == 'Low' and \
                       row[4] == 'Close' and row[5] == 'Volume' and \
                       row[6] == 'Adj Close'
                count += 1
            else:
                data.append( ( Sponta.parseDate( row[0], toTimestamp=True ),
                               float( row[1] ),
                               float( row[2] ),
                               float( row[3] ),
                               float( row[4] ),
                               int( row[5] ),
                               float( row[6] ) ) )
                count += 1
        return data

    def getQuote( self, symbols ):
        '''
        Get current quotes from Yahoo. Length of symbols must be <= 10.
    
        @return a hash. Key is symbol. Value is a has of quote fields:
                Symbol, Feed, Last, LastTradeDate, Open, PrevClose, DayHigh,
                DayLow, Volume, AvgVolume, High52, Low52, Timestamp
        '''
        
        assert len( symbols ) < 11
        
        baseUrl = "http://download.finance.yahoo.com/d/quotes.csv?"
        if isinstance( symbols, str ):
            symbols = "s=%s" % symbols
        else:
            symbols = "s=%s" % ",".join( symbols )

        fields = "f=%s" % "".join( [ f[1] for f in YahooQuoteMapping ] )
        url = "%s%s&%s" % ( baseUrl, symbols, fields )

        reader = self.openCsvUrl( url )
        if reader is None:
            logMessage( "Fetch quote failed for %s: %s" % (
                    symbol, self.error_ ) )
            return None

        timestamp = int( time.time() )
        
        data = {}
        for row in reader:
            if len( row ) != len( YahooQuoteMapping ):
                break
                
            if ( row[ YahooFieldIndex[Volume] ] == "N/A" or 
                 row[ YahooFieldIndex[ AvgVolume ] ] == "N/A" or 
                 int( row[ YahooFieldIndex[ AvgVolume ] ] ) == 0 ):
                continue # no quote
                
            quote = { Symbol : str( row[0] ) }
            # Combine LastDate and LastTime to LastTradeDate
            dt = Sponta.parseDate( row[ YahooFieldIndex[LastDate] ],
                                   toTimestamp=True )
            dt += Sponta.parseTime( row[ YahooFieldIndex[LastTime] ] )
            quote[ LastTradeDate] = int( dt )

            # Convert rest of fields to proper types
            for field in YahooFieldIndex:
                if field in quote:
                    continue
                if not field in TypesMapping:
                    continue
                index = YahooFieldIndex[ field ]
                if TypesMapping[ field ] == 'i':
                    if row[index] == 'N/A':
                        quote[ field ] = -1
                    else:
                        quote[ field ] = int( row[index] )
                elif TypesMapping[ field ] == 'f':
                    if row[index] == 'N/A':
                        quote[ field ] = -1
                    else:
                        quote[ field ] = float( row[index] )
                else:
                    quote[ field ] = str( row[index] )

            quote[ Feed ] = "yahoo"
            quote[ Timestamp ] = timestamp
            data[ quote[ Symbol ] ] = quote

        if len( data ) >  0:
            return data
        
        logMessage( "Fetch quote failed for %s" % symbols )
        return None

#------------------------------------------------------------------------------
# Time series database wrapper. It is for historical data.
#
# Schema is time_series.sql. Table is time_series. Columns:
#    
#   ( symbol, last_date, open_price, day_high, day_low, last_price,    
#     volume, adj_close, timestamp )
#     
#------------------------------------------------------------------------------
class TimeSeries:

    def __init__( self, symbol, store=None ):
        self.symbol_ = symbol
        self.store_ = Sponta.timeSeriesStore( symbol ) \
            if store is None else store
        self.db_ = Database( self.store_, schema="time_series.sql" )

    def getData( self, startDate, endDate=None ):
        startStamp = Sponta.parseDate( startDate, toTimestamp=True )
        if endDate is None:
            endDate = datetime.date.today()
        endStamp = Sponta.parseDate( endDate, toTimestamp=True )
        sql = "select * from time_series where last_date >= %s " + \
              "and last_date <= %s" % ( startStamp, endStamp )
        return self.db_.runQuery( sql, fetchAll=True )

    def getDataBack( self, backtrack ):
        sql = "select * from time_series order by timestamp limit %s" % backtrack
        data = self.db_.runQuery( sql, fetchAll=True )
        import pdb
        pdb.set_trace()
        return data
    
    def saveData( self, data ):
        '''
        data must be an ascending continguous time series and has all columns
        of time_series table. 
        '''
        # Sample sanity check
        timestamp = 0
        for row in data:
            if len( row ) != 7:
                logMessage( "Invalid data: %s" % row )
                return False
            rowTimestamp = Sponta.parseDate( row[0], toTimestamp=True )
            if timestamp > rowTimestamp:
                logMessage( "Invalid data timestamp %s:%s" % (
                        timestamp, row[0] ) )
                return False
            timestamp = rowTimestamp
            for value in row[1:]:
                if int( value ) != float( value ):
                    logMessage( "Invalid data. Unnormalized: %s" % row )
                    return False
                
        # The last timestamp must be >= current database timestamp
        sql = "select max( timestamp ) from time_series"
        result = self.db_.runQuery( sql )
        if result:
            if timestamp < result[0]:
                logMessage( "Invalid data. Timestamp < db. %s:%s" % (
                        timestamp, result[0] ) )
                return False
            # Delete existing rows
            sql = "delete from time_series where timestamp >= %r" % data[0][0]
            self.db_.executeSql( sql )

        # Insert data
        qmarks = "?," * 7
        sql = "insert into time_series values ( %s )" % qmarks.rstrip(",")
        return self.db_.executeMany( sql, data )

    def loadCSV( self, csvFile ):
        '''Load CSV file into time series database. This is for testing.'''
        fh = open( csvFile, "r" )
        if not fh:
            Logging.logMessage( "Cannot open file %s" % csvFile )
            return False
        reader = csv.reader( fh )
        data = []
        for row in reader:
            if len( row ) != 7:
                Logging.logMessage( "Invalid csv line: %s" % row )
                return False
            data.append( row )
        return self.saveData( data )
            
#------------------------------------------------------------------------------
# Current market quote fetcher
#
# We store real-time quotes in quoteStore. We use stored quotes if it is not
# stale, unless forced to fetch again.
#
# @return a hash of quotes. Key is symbol. Value is a hash of QuoteFields.
#    
#------------------------------------------------------------------------------    
class QuoteFetcher:

    def __init__( self, store=Sponta.quoteStore, feed='yahoo' ):
        self.db_ = Database( store, schema="market_quote.sql" )
        self.feed_ = Yahoo()

    def getQuotes( self, symbols, force=False ):
        
        data = {}
        toFetch = set([])

        if not force:
            results = self.getQuotesFromDb_( symbols )
            if results:
                data = results
                toFetch = set( symbols ).difference( set( results ) )
            else:
                toFetch = symbols
        else:
            toFetch = symbols
            
        if len( toFetch ) > 0:
            results = self.getQuotesFromFeed_( list( toFetch ) )
            if results:
                data.update( results )

        return data

    def getQuotesFromDb_( self, symbols, earliest=180 ):

        query = "select %s from market_quote " + \
                "where %s in ( %s ) and %s = ? and %s > ?"
        
        timestamp = int( time.time() ) - earliest if earliest else 0
        symbols = ", ".join( [ "%r" % s for s in symbols ] )
        binding = ( "yahoo", timestamp )
        results = self.db_.runQuery( query % ( ", ".join( QuoteFields ),
                                               Symbol, symbols,
                                               Feed, Timestamp ),
                                     binding=binding, fetchAll=True )
        if len( results ) > 0:
            data = {}
            for row in results:
                data[ row[Symbol] ] = row
            return data

        return None

    def getQuotesFromFeed_( self, symbols ):
        
        results = {} # hash key is symbol
        i = 0
        while i < len( symbols ):
            data = self.feed_.getQuote( symbols[ i:i+10 ] )
            if data:
                results.update( data )
            i += 10

        if len( results ) == 0:
            return None
        
        values = [] # for insert into db
        for symbol in results:
            row = results[ symbol ]
            values.append( [ row[f] for f in QuoteFields ] )

        invalids = set( symbols ).difference( set( results ) )
        if len( invalids ) > 0:
            logMessage( "Failed to get quotes for %s" % ",".join( invalids ) )
            for sym in invalids:
                del results[ sym ]
            
        # delete existing entries in database. 10 per batch.
        i = 0
        resultSyms = results.keys()
        while i < len( resultSyms ):
            syms = resultSyms[ i:i+10 ]
            sql = "delete from market_quote where symbol in ( %s )" % (
                  ", ".join( [ '%r' % s for s in syms ] ) )
            self.db_.runQuery( sql )
            i += 10

        # insert into market_quote table
        i = 0
        while i < len( values ):
            vals = values[ i:i+10 ]
            qmarks = "?," * len( QuoteFields )
            sql = "insert into market_quote ( %s ) values ( %s )" % (
                  ",".join( QuoteFields ), qmarks.rstrip( ',' ) )
            self.db_.executeMany( sql, vals )
            i += 10

        return results

    def resetStore( self ):
        self.db_.resetDatabase()
        
#------------------------------------------------------------------------------
# Quote Data Set
#
#------------------------------------------------------------------------------
class QuoteData( TimeSeries ):
    def __init__( self, symbol ):
        TimeSeries.__init__( self, symbol )

    def getDataBack( self, backtrack ):
        date = TimeSeries.getDataBack( self, backtrack )
        if isTradingTime():
            quote = self.quoteFetcher_.getQuote( self.symbol )
        else:
            day = lastTradingDay( toTimestamp=True )
            if data[0]['timestamp'] < day:
                startDate = Sponta.nextDay( data[0]['timestamp'] )
                quote = self.feed_.getHistoricalPrices(
                    self.symbol, startDate, day )
                self.saveData( quote )
            

            
        
