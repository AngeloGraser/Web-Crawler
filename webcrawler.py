"""
Example webcrawler or 'spider' code. This spider integrates both of the code routines provided including:

PorterStemmer - this code provided in Unit 4 implements a porter stemmer. We have integrated this code into the program and called the stem method. This routine could have been implemented as a library and included into the program but for the sake of simplicity an clarity when debugging, I have simply included the source code into my web crawler script. You can find this code in the resources section for this
unit.

BeautifulSoup - this code which can be found in the resources section of this unit is a python module
that allows text to be read from a html page. Essentially it returns the text of the html page with all of the HTML tags and other formatting removed making providing a simple string containing the contents of a web page
that can be parsed and indexed by our indexer code. This module can be downloaded from unit resources section which also has instructions for installing the module on your system.

"""
import sys, os, re
from urllib.request import Request, urlopen
import urllib.error
from urllib.parse import urlparse, urlunparse
import sqlite3
import math
import time
import requests
from bs4 import BeautifulSoup, NavigableString

stopwords = ['the', 'of', 'and', 'to', 'in', 'you', 'it', 'with', 'that', 'or', 'was', 'he', 'is', 'for', 'this', 'his', 'as', 'not', 'at', 'by', 'all', 'they', 'but', 'be', 'on', 'from', 'had', 'her','work', 'are', 'any', 'she', 'if', 'said', 'so', 'which', 'have', 'do', 'we', 'no', 'my', 'were', 'them', 'their', 'him', 'one', 'will', 'me', 'there', 'who', 'up', 'other', 'an', 'its','when', 'what', 'can', 'may', 'into', 'out', 'must', 'your', 'then', 'would', 'could', 'more', 'now', 'has', 'like', 'down', 'where', 'been', 'through', 'did', 'away', 'these','such', 'set', 'back', 'some', 'than', 'way', 'made', 'our', 'after', 'well', 'should', 'get', 'even', 'am', 'go', 'saw', 'just', 'put', 'while', 'ever', 'off', 'here', 'also']

# regular expression for: extract words, extract ID from path, check for hexa value
chars = re.compile(r'\W+')
pattid= re.compile(r'(\d{3})/(\d{3})/(\d{3})')


# the higher ID
tokens = 0
documents = 0
terms = 0
stopword_count = 0

#
# We will create a term object for each unique instance of a term
#
class Term():
        termid = 0
        termfreq = 0
        docs = 0
        docids = {}

# split on any chars
def splitchars(line) :
        return chars.split(line)

# Unused method replaced by BeutifulSoup
def stripTags(s):
    intag = False
    s2 = ""
    for c in s:
        if c == '<':
            intag = True
        elif c == '>':
            intag = False
        if intag != True:
            s2 = s2+c
    return(s2)

def printText(tags):
        for tag in tags:
                if tag.__class == NavigableString:
                       print(tag)
                else:
                       printText(tag)
        print("")


# process the tokens of the source code
def parsetoken(db, line):
        global documents
        global tokens
        global terms
        global stopword_count

        # Create instance of the porterstemmer object we will call the stemmer method in this
        # object to 'stem' the tokens extracted from the line.
        p = PorterStemmer()

        # this replaces any tab characters with a space character in the line read from the file
        line = line.replace('\t',' ')
        line = line.strip()

        # This routine splits the contents of the line into tokens
        l = splitchars(line)

        # for each token in the line process
        # Note: Process mirrors that of the search engine to be used for it
        for elmt in l:
            # This statement removes the newline character if found
            elmt = elmt.replace('\n','')

            # This statement converts all letters to lower case
            lowerElmt = elmt.lower().strip()

            # Increment the counter of the number of tokens processed. This value will
            # provide the total size of the corpus in terms of the number of terms in the
            # entire collection
            tokens += 1

            # if the token is less than 2 characters in length we assume
            # that it is not a valid term and ignore it
            if len(lowerElmt) <2:
                continue

            # if the token is in the stopwords list then do not include in the term
            # dictionary and do not index the term.
            # Updated to include stopword counter
            if (lowerElmt in stopwords):
                stopword_count += 1
                continue

            # This section of code will check to see if the term is a number and will not
            # add a number to the index. This is accomplished by attempting to convert
            # the term into an integer and assigning it to a variable. If the term is not
            # a number meaning it contains non numeric characters this will fail and we can
            # catch this error and continue processing the term. If the term is a number
            # it will not fail and we can then ignore the term (the continue statement will
            # continue with the next item retrieved from the 'for' statement)
            try:
                dummy = int(lowerElmt)
            except ValueError:
                # Value is not a number so we can index it
                stemword = lowerElmt
            else:
                # Value is a number so we will NOT add it to the index
                continue

            # In this following section of the code the porter stemmer code is called for the indexer process.
            # This algorithm will stem the tokens which will reduce the size of our data dictionary.
            lowerElmt = p.stem(stemword, 0,len(stemword)-1)

            # If the term doesn't currently exist in the term dictionary then add the term
            if not (lowerElmt in db.keys()):
                terms+=1
                db[lowerElmt] = Term()
                db[lowerElmt].termid = terms
                db[lowerElmt].docids = dict()
                db[lowerElmt].docs = 0

            # If the document is not currently in the postings list for the term then add it
            if not (documents in db[lowerElmt].docids.keys()):
                db[lowerElmt].docs += 1
                db[lowerElmt].docids[documents] = 0

            # Increment the counter that tracks the term frequency
            db[lowerElmt].docids[documents] += 1
        return l

# Create the inverted index tables.
# Insert a row into the TermDictionary for each unique term along with a termid which is an integer assigned to each
# term by incrementing an integer
# Insert a row into the posting table for each unique combination of Docid and termid
def writeindex(db):
        for k in db.keys():
                cur.execute('insert into TermDictionary values (?,?)', (k, db[k].termid))
                docfreq = db[k].docs
                ratio = float(documents) / float(docfreq)
                idf = math.log10(ratio)

                for i in db[k].docids.keys():
                        termfreq = db[k].docids[i]
                        tfidf = float(termfreq) * float(idf)
                        if tfidf > 0:
                                cur.execute('insert into Posting values (?, ?, ?, ?, ?)', (db[k].termid, i, tfidf, docfreq, termfreq))

# Normalises url names to avoid duplicates in the queue and DocumentDictionary
def norm_url(url):
    parsed = urllib.parse.urlparse(url)
    scheme = parsed.scheme.lower()
    netlock = parsed.netloc.lower()
    path = parsed.path
    if not path:
        path = "/"
    elif path.endswith("/") and path != "/":
        path = path.rstrip("/")
    return urlunparse((scheme, netlock, path, "", "", ""))

"""
=========================================================================================
Porter Stemming Algorithm
This is the Porter stemming algorithm, ported to Python from the version coded up in ANSI C by the author. It may be be regarded as canonical, in that it follows the algorithm presented in

Porter, 1980, An algorithm for suffix stripping, Program, Vol. 14, no. 3, pp 130-137,

only differing from it at the points maked --DEPARTURE-- below. See also http://www.tartarus.org/~martin/PorterStemmer
The algorithm as described in the paper could be exactly replicated by adjusting the points of DEPARTURE, but this is barely necessary,
because (a) the points of DEPARTURE are definitely improvements, and
(b) no encoding of the Porter stemmer I have seen is anything like as exact as this version, even with the points of DEPARTURE!

Vivake Gupta (v@nano.com) Release 1: January 2001
Further adjustments by Santiago Bruno (bananabruno@gmail.com) to allow word input not restricted to one word per line, leading

to:

release 2: July 2008
========================================================================================      
"""

class PorterStemmer:

    def __init__(self):
        """The main part of the stemming algorithm starts here.
        b is a buffer holding a word to be stemmed. The letters are in b[k0],
        b[k0+1] ... ending at b[k]. In fact k0 = 0 in this demo program. k is
        readjusted downwards as the stemming progresses. Zero termination is
        not in fact used in the algorithm.

        Note that only lower case sequences are stemmed. Forcing to lower case
        should be done before stem(...) is called.
        """

        self.b = ""  # buffer for word to be stemmed
        self.k = 0
        self.k0 = 0
        self.j = 0  # j is a general offset into the string

    def cons(self, i):
        """cons(i) is TRUE <=> b[i] is a consonant."""
        if self.b[i] == 'a' or self.b[i] == 'e' or self.b[i] == 'i' or self.b[i] == 'o' or self.b[i] == 'u':
            return 0
        if self.b[i] == 'y':
            if i == self.k0:
                return 1
            else:
                return (not self.cons(i - 1))
        return 1

    def m(self):
        """m() measures the number of consonant sequences between k0 and j.
        if c is a consonant sequence and v a vowel sequence, and <..>
        indicates arbitrary presence,

           <c><v>       gives 0
           <c>vc<v>     gives 1
           <c>vcvc<v>   gives 2
           <c>vcvcvc<v> gives 3
           ....
        """
        n = 0
        i = self.k0
        while 1:
            if i > self.j:
                return n
            if not self.cons(i):
                break
            i = i + 1
        i = i + 1
        while 1:
            while 1:
                if i > self.j:
                    return n
                if self.cons(i):
                    break
                i = i + 1
            i = i + 1
            n = n + 1
            while 1:
                if i > self.j:
                    return n
                if not self.cons(i):
                    break
                i = i + 1
            i = i + 1

    def vowelinstem(self):
        """vowelinstem() is TRUE <=> k0,...j contains a vowel"""
        for i in range(self.k0, self.j + 1):
            if not self.cons(i):
                return 1
        return 0

    def doublec(self, j):
        """doublec(j) is TRUE <=> j,(j-1) contain a double consonant."""
        if j < (self.k0 + 1):
            return 0
        if (self.b[j] != self.b[j - 1]):
            return 0
        return self.cons(j)

    def cvc(self, i):
        """cvc(i) is TRUE <=> i-2,i-1,i has the form consonant - vowel - consonant
        and also if the second c is not w,x or y. this is used when trying to
        restore an e at the end of a short  e.g.

           cav(e), lov(e), hop(e), crim(e), but
           snow, box, tray.
        """
        if i < (self.k0 + 2) or not self.cons(i) or self.cons(i - 1) or not self.cons(i - 2):
            return 0
        ch = self.b[i]
        if ch == 'w' or ch == 'x' or ch == 'y':
            return 0
        return 1

    def ends(self, s):
        """ends(s) is TRUE <=> k0,...k ends with the string s."""
        length = len(s)
        if s[length - 1] != self.b[self.k]:  # tiny speed-up
            return 0
        if length > (self.k - self.k0 + 1):
            return 0
        if self.b[self.k - length + 1:self.k + 1] != s:
            return 0
        self.j = self.k - length
        return 1

    def setto(self, s):
        """setto(s) sets (j+1),...k to the characters in the string s, readjusting k."""
        length = len(s)
        self.b = self.b[:self.j + 1] + s + self.b[self.j + length + 1:]
        self.k = self.j + length

    def r(self, s):
        """r(s) is used further down."""
        if self.m() > 0:
            self.setto(s)

    def step1ab(self):
        """step1ab() gets rid of plurals and -ed or -ing. e.g.

           caresses  ->  caress
           ponies    ->  poni
           ties      ->  ti
           caress    ->  caress
           cats      ->  cat

           feed      ->  feed
           agreed    ->  agree
           disabled  ->  disable

           matting   ->  mat
           mating    ->  mate
           meeting   ->  meet
           milling   ->  mill
           messing   ->  mess

           meetings  ->  meet
        """
        if self.b[self.k] == 's':
            if self.ends("sses"):
                self.k = self.k - 2
            elif self.ends("ies"):
                self.setto("i")
            elif self.b[self.k - 1] != 's':
                self.k = self.k - 1
        if self.ends("eed"):
            if self.m() > 0:
                self.k = self.k - 1
        elif (self.ends("ed") or self.ends("ing")) and self.vowelinstem():
            self.k = self.j
            if self.ends("at"):
                self.setto("ate")
            elif self.ends("bl"):
                self.setto("ble")
            elif self.ends("iz"):
                self.setto("ize")
            elif self.doublec(self.k):
                self.k = self.k - 1
                ch = self.b[self.k]
                if ch == 'l' or ch == 's' or ch == 'z':
                    self.k = self.k + 1
            elif (self.m() == 1 and self.cvc(self.k)):
                self.setto("e")

    def step1c(self):
        """step1c() turns terminal y to i when there is another vowel in the stem."""
        if (self.ends("y") and self.vowelinstem()):
            self.b = self.b[:self.k] + 'i' + self.b[self.k + 1:]

    def step2(self):
        """step2() maps double suffices to single ones.
        so -ization ( = -ize plus -ation) maps to -ize etc. note that the
        string before the suffix must give m() > 0.
        """
        if self.b[self.k - 1] == 'a':
            if self.ends("ational"):
                self.r("ate")
            elif self.ends("tional"):
                self.r("tion")
        elif self.b[self.k - 1] == 'c':
            if self.ends("enci"):
                self.r("ence")
            elif self.ends("anci"):
                self.r("ance")
        elif self.b[self.k - 1] == 'e':
            if self.ends("izer"):      self.r("ize")
        elif self.b[self.k - 1] == 'l':
            if self.ends("bli"):
                self.r("ble")  # --DEPARTURE--
            # To match the published algorithm, replace this phrase with
            #   if self.ends("abli"):      self.r("able")
            elif self.ends("alli"):
                self.r("al")
            elif self.ends("entli"):
                self.r("ent")
            elif self.ends("eli"):
                self.r("e")
            elif self.ends("ousli"):
                self.r("ous")
        elif self.b[self.k - 1] == 'o':
            if self.ends("ization"):
                self.r("ize")
            elif self.ends("ation"):
                self.r("ate")
            elif self.ends("ator"):
                self.r("ate")
        elif self.b[self.k - 1] == 's':
            if self.ends("alism"):
                self.r("al")
            elif self.ends("iveness"):
                self.r("ive")
            elif self.ends("fulness"):
                self.r("ful")
            elif self.ends("ousness"):
                self.r("ous")
        elif self.b[self.k - 1] == 't':
            if self.ends("aliti"):
                self.r("al")
            elif self.ends("iviti"):
                self.r("ive")
            elif self.ends("biliti"):
                self.r("ble")
        elif self.b[self.k - 1] == 'g':  # --DEPARTURE--
            if self.ends("logi"):      self.r("log")
        # To match the published algorithm, delete this phrase

    def step3(self):
        """step3() dels with -ic-, -full, -ness etc. similar strategy to step2."""
        if self.b[self.k] == 'e':
            if self.ends("icate"):
                self.r("ic")
            elif self.ends("ative"):
                self.r("")
            elif self.ends("alize"):
                self.r("al")
        elif self.b[self.k] == 'i':
            if self.ends("iciti"):     self.r("ic")
        elif self.b[self.k] == 'l':
            if self.ends("ical"):
                self.r("ic")
            elif self.ends("ful"):
                self.r("")
        elif self.b[self.k] == 's':
            if self.ends("ness"):      self.r("")

    def step4(self):
        """step4() takes off -ant, -ence etc., in context <c>vcvc<v>."""
        if self.b[self.k - 1] == 'a':
            if self.ends("al"):
                pass
            else:
                return
        elif self.b[self.k - 1] == 'c':
            if self.ends("ance"):
                pass
            elif self.ends("ence"):
                pass
            else:
                return
        elif self.b[self.k - 1] == 'e':
            if self.ends("er"):
                pass
            else:
                return
        elif self.b[self.k - 1] == 'i':
            if self.ends("ic"):
                pass
            else:
                return
        elif self.b[self.k - 1] == 'l':
            if self.ends("able"):
                pass
            elif self.ends("ible"):
                pass
            else:
                return
        elif self.b[self.k - 1] == 'n':
            if self.ends("ant"):
                pass
            elif self.ends("ement"):
                pass
            elif self.ends("ment"):
                pass
            elif self.ends("ent"):
                pass
            else:
                return
        elif self.b[self.k - 1] == 'o':
            if self.ends("ion") and (self.b[self.j] == 's' or self.b[self.j] == 't'):
                pass
            elif self.ends("ou"):
                pass
            # takes care of -ous
            else:
                return
        elif self.b[self.k - 1] == 's':
            if self.ends("ism"):
                pass
            else:
                return
        elif self.b[self.k - 1] == 't':
            if self.ends("ate"):
                pass
            elif self.ends("iti"):
                pass
            else:
                return
        elif self.b[self.k - 1] == 'u':
            if self.ends("ous"):
                pass
            else:
                return
        elif self.b[self.k - 1] == 'v':
            if self.ends("ive"):
                pass
            else:
                return
        elif self.b[self.k - 1] == 'z':
            if self.ends("ize"):
                pass
            else:
                return
        else:
            return
        if self.m() > 1:
            self.k = self.j

    def step5(self):
        """step5() removes a final -e if m() > 1, and changes -ll to -l if
        m() > 1.
        """
        self.j = self.k
        if self.b[self.k] == 'e':
            a = self.m()
            if a > 1 or (a == 1 and not self.cvc(self.k - 1)):
                self.k = self.k - 1
        if self.b[self.k] == 'l' and self.doublec(self.k) and self.m() > 1:
            self.k = self.k - 1

    def stem(self, p, i, j):
        """In stem(p,i,j), p is a char pointer, and the string to be stemmed
        is from p[i] to p[j] inclusive. Typically i is zero and j is the
        offset to the last character of a string, (p[j+1] == '\0'). The
        stemmer adjusts the characters p[i] ... p[j] and returns the new
        end-point of the string, k. Stemming never increases word length, so
        i <= k <= j. To turn the stemmer into a module, declare 'stem' as
        extern, and delete the remainder of this file.
        """
        # copy the parameters into statics
        self.b = p
        self.k = j
        self.k0 = i
        if self.k <= self.k0 + 1:
            return self.b  # --DEPARTURE--

        # With this line, strings of length 1 or 2 don't go through the
        # stemming process, although no mention is made of this in the
        # published algorithm. Remove the line to match the published
        # algorithm.

        self.step1ab()
        self.step1c()
        self.step2()
        self.step3()
        self.step4()
        self.step5()
        return self.b[self.k0:self.k + 1]

if __name__ == '__main__':

    # Get the starting URL to crawl
    line = input("Enter URL to crawl (must be in the form http://www.domain.com): ")

    # the database is a simple dictionary
    db = {}

    # Capture the start time of the routine so that we can determine the total running
    # time required to process the corpus
    t2 = time.localtime()
    print ('Start Time: %.2d:%.2d' % (t2.tm_hour, t2.tm_min))


    # Create a sqlite database to hold the inverted index. The isolation_level statment turns
    # on autocommit which means that changes made in the database are committed automatically
    con = sqlite3.connect("C:\\Users\\Admin\\OneDrive\\Development\\Python Workspace\\CS3308Unit7\\webcrawler.db")  # path to be edited
    con.isolation_level = None
    cur = con.cursor()

    # In the following section three tables and their associated indexes will be created.
    # Before we create the table or index we will attempt to drop any existing tables in
    # case they exist
    # Note: DB is compatible with how the search engine being used will interoperate it

    # Document Dictionary Table
    cur.execute("drop table if exists DocumentDictionary")
    cur.execute("drop index if exists idxDocumentDictionary")
    cur.execute("create table if not exists DocumentDictionary (DocumentName text, DocId int)")
    cur.execute("create index if not exists idxDocumentDictionary on DocumentDictionary (DocId)")

    # Term Dictionary Table
    cur.execute("drop table if exists TermDictionary")
    cur.execute("drop index if exists idxTermDictionary")
    cur.execute("create table if not exists TermDictionary (Term text, TermId int)")
    cur.execute("create index if not exists idxTermDictionary on TermDictionary (TermId)")

    # Postings Table
    cur.execute("drop table if exists Posting")
    cur.execute("drop index if exists idxPosting1")
    cur.execute("drop index if exists idxPosting2")
    cur.execute("create table if not exists Posting (TermId int, DocId int, tfidf real, docfreq int, termfreq int)")
    cur.execute("create index if not exists idxPosting1 on Posting (TermId)")
    cur.execute("create index if not exists idxPosting2 on Posting (Docid)")

    # Initialize variables
    # Contains the list of pages that have already been crawled
    crawled = ([])
    # Contains the queue of url's that will be crawled
    tocrawl = [line]
    # Counts the number of links in the queue to limit the depth of the crawl
    links_queue = 0
    # Flat that will exit the while loop when the craw is finished
    crawlcomplete = True

    # Crawl the starting web page and links in the web page up to the limit.
    while crawlcomplete:

        # Pop the top url off of the queue and process it.
        try:
                crawling = tocrawl.pop(0)
                # Crawl is breadth first i.e. explores surface pages before in-depth branches
                # Prevents eternal heavy crawling
        except:
                crawlcomplete = False
                continue

        # Normalise the url
        crawling = norm_url(crawling)

        # Skip this document if already indexed
        if crawling in crawled:
            continue

        l = len(crawling)
        ext = crawling[l-4:l]
        if ext in ['.pdf', '.png', '.jpg', '.gif', '.asp']:
                crawled.append(crawling)
                continue

        # Print the current length of the queue of URL's to crawl
        print(len(tocrawl),crawling)

        # Parse the URL and open it.
        # Updated to use requests library to avoid errors due to crawler detection and page redirects
        url = urlparse(crawling)
        try:
            response = requests.get(crawling, headers={'User-Agent': 'Mozilla/5.0'}, timeout=5)
            html = response.text
            # NOTE: Old parser couldn't handle redirects. Output was also in byte code.
            #req = Request(crawling,headers={"User-Agent": "Mozilla/5.0"})
            #response = urlopen(req).read()
        except:
                print(f"Failed crawling (404 or 403) {crawling}")
                # Page was inaccessible, likely forbidden 403 or not found 404
                continue

        # Included decode bytes to text for parsing
        # NOTE: Removed after updating url parser above. response is now soup object, was byte code.
        #try:
            #html = response.decode(errors="ignore")
        #except:
            #continue

        # Use BeautifulSoup modules to format web page as text that can be parsed and indexed
        # Updated for Python 3 and current BeautifulSoup module
        soup = BeautifulSoup(html, "html.parser")
        paragraphs = soup.find_all('p', string=re.compile("."))
        # Extracts only content text within <p> tags of a page, ignoring all other html.
        tok = "".join(p.get_text() for p in paragraphs)

        # pass the text extracted from the web page to the parsetoken routine for indexing
        parsetoken(db, tok)
        documents += 1

        # For each unique instance of a document assign a document id (documents) and store in the documentdictionary
        # Updated for correct insert order
        cur.execute("insert into DocumentDictionary values (?, ?)", (crawling, documents))

        # Find all the weblinks on the page put them in the stack to crawl through
        # Updated loop to use a propper url format (prevents issues passing HTTPS as HTTP)
        # Also prevents splitting pages into separate documents
        scheme = url.scheme
        domain = url.netloc

        if links_queue < 500:
                # Updated url filter
                links = re.findall(r'href=["\']([^"\']+)["\']', html, re.I)
                for link in links:
                    if link.startswith('#') or link.startswith('javascript:') or link.startswith('mailto:'):
                        #link = 'http://' + url[1] + url[2] + link
                        # Statement generated unusable url links as fragments
                        # that could duplicate URL indexing as documents.
                        # Now avoids fragment only links, script files, and mailto links.
                        continue
                    elif link.startswith('/'):
                        #link = 'http://' + url[1] + link
                        # Update to convert relative URLs to absolute links
                        link = f"{url.scheme}://{url.netloc}{link}"
                    elif link.startswith('//'):
                        #Handles protocol relative URLs by inserting https: at the start
                        # (i.e. //site.com/page.html to https://site.com/page.html)
                        link = f"{url.scheme}:{link}"
                    elif link.startswith('http'):
                        #link = 'http://' + url[1] + '/' + link
                        #link = f"{scheme}://{domain}{link}"
                        # Updated to avoid queuing broken URLs (passes and keeps http and https links).
                        # New iteration above handles original function.
                        pass
                    else:
                        # Skip malformed URLs from potential link fragmentation
                        continue
                    # do not queue duplicates (check normalised url)
                    link = norm_url(link)
                    # Updated to avoid queuing duplicate urls
                    if link not in crawled and link not in tocrawl:
                        links_queue += 1
                        tocrawl.append(link)
        crawled.append(crawling)

    # Display the time that the indexing process is complete, and the process of writing
    t2 = time.localtime()
    print ('Indexing Complete, write to disk: %.2d:%.2d' % (t2.tm_hour, t2.tm_min))

    # Write the inverted index to disk
    writeindex(db)

    # Commit and close the database
    con.commit()
    con.close()

    # Print processing statistics
    # Documents - every document opened and read by the indexer
    # Terms - each token that was extracted from the file.
    print ("Documents %i" % documents)
    print ("Terms %i" % terms)
    print ("Tokens %i" % tokens)
    print ("Stopwords %i" % stopword_count)
    t2 = time.localtime()
    print ('End Time: %.2d:%.2d' % (t2.tm_hour, t2.tm_min))