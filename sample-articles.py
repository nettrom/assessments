#!/usr/env/python
# -*- coding: utf-8 -*-
"""
Script to sample random assessment articles from a set of
assessment classes using the database.  Input is a tab-separated
file with two columns:

1: Assessment class name (one of FA, GA, A, B, C, Start, or Stub)
2: number of articles to sample

A line in the input file starting with "#" will be ignored.

The script will then sample one or two sets of articles from each
group (command line option '-t' will make it sample two (training
and test) sets).

Output is a tab-separated file with two columns:

1: shortname
2: pageid

Categories are distinct, an article will not be found in multiple categories.

Articles that are sampled should not be:

1: a redirect
2: a disambiguation page
3: a list
"""

import os;
import re;
import codecs;
import random;

import MySQLdb;
from MySQLdb import cursors;

import logging;

class ArticleSampler:
    def __init__(self, sampleConfigFile=None, outputFilename=None,
                 sampleTestSet=False,
                 cutoffDate=None):

        self.dbHost = 'enwiki.labsdb';
        self.dbName = 'enwiki_p';
        self.dbConn = None;
        self.dbCursor = None;
        self.dbConf = '~/replica.my.cnf';

        # Are we sampling a test set too?
        self.sampleTestSet = sampleTestSet;

        # Do we only accept articles created before a given date?
        self.cutoffDate = cutoffDate;

        # Set of page IDs for articles we've already sampled
        self.alreadySampled = set();
        
        # Mapping used for setting sort values, used for prioritising
        # article selection.
        sortMap = {'FA': 0,
                   'A': 1,
                   'GA': 2,
                   'B': 3,
                   'C': 4,
                   'Start': 5,
                   'Stub': 6};

        self.outputFilename = "sampled-assessment-articles";
        if outputFilename:
            self.outputFilename = outputFilename;

        configFile = 'sample-setup.txt';
        if sampleConfigFile:
            configFile = sampleConfigFile;

        # Open the input file and read category configurations
        self.catConfig = [];
        with codecs.open(os.path.expanduser(configFile), 'r', 'utf-8') as inFile:
            for line in inFile:
                line = line.strip();
                if re.match('#', line):
                    continue;
                (classname, n) = line.split('\t');
                self.catConfig.append({'classname': classname,
                                       'narticles': int(n),
                                       'sortkey': sortMap[classname]});

        self.seenCount = 0;

    def connect(self):
        '''
        Open the database connection.
        '''
        try:
            self.dbConn = MySQLdb.connect(host=self.dbHost,
                                          db=self.dbName,
                                          use_unicode=True,
                                          read_default_file=os.path.expanduser(self.dbConf));
            # Create an SSDictCursor, standard fare.
            self.dbCursor = self.dbConn.cursor(cursors.SSDictCursor);
        except MySQLdb.Error, e:
            logging.error("Unable to connect to database: {code} {explain}".format(code=e.args[0], explain=e.args[1]));
            self.dbConn = None;
            self.dbCursor = None;
            
        if self.dbConn:
            return True;
        else:
            return False;

    def disconnect(self):
        '''
        Close the database connection.
        '''
        try:
            self.dbCursor.close();
            self.dbConn.close();
        except:
            pass;

        return;

    def getRandomStubCategory(self):
        """
        Get a random stub category.
        """

        # Query to fetch a number of random pages from a given category.
        # (from revision c8f34ea of opentasks.py)
        randomPageQuery = r"""SELECT
                              page_id, page_title
                              FROM page JOIN categorylinks ON page_id=cl_from
                              WHERE cl_to=%(category)s
                              AND page_namespace=%(namespace)s
                              AND page_random >= RAND()
                              ORDER BY page_random
                              LIMIT %(limit)s""";

        category = self.categories['stub'];
        foundArticle = None;

        randStubCategory = None;
        broken = False;
        while not randStubCategory and not broken: # while not done...
            # pick a random stub category
            attempts = 0;
            while attempts < self.maxDBQueryAttempts:
                try:
                    # pick one random stub category (ns = 14)
                    self.dbCursor.execute(randomPageQuery,
                                          {'category': re.sub(" ", "_", category).encode('utf-8'),
                                           'namespace': 14,
                                           'limit': 1});
                    for row in self.dbCursor.fetchall():
                        randStubCategory = unicode(row['page_title'], 'utf-8', errors='strict');
                except MySQLdb.Error, e:
                    attempts += 1;
                    logging.warning("Error: Unable to execute query to get a random stub category, possibly retrying!\n");
                    logging.warning("Error {0}: {1}\n".format(e.args[0], e.args[1]));
                    if e.errno == MySQLdb.constants.CR.SERVER_GONE_ERROR \
                            or e.errno == MySQLdb.constants.CR.SERVER_LOST:
                        # lost connection, reconnect
                        self.connectDatabase();
                else: 
                    break;
                    
            if not randStubCategory:
                # something went wrong
                logging.error("Error: Unable to find random stub category, aborting!\n");
                broken = True;
                continue;

        logging.info(u"Selected random stub category {cat}".format(cat=randStubCategory));
        return randStubCategory;

    def getAssessmentClassArticles(self, assessmentClass=u'FA'):
        """
        Get all articles from the given assessment class.

        An article's title can not start with "List of", or contain "(disambiguation)".
        Single redirects within the Main namespace are followed, and it is assumed that
        the redirect falls into the same assessment class.

        @param assessmentClass: short name (e.g. "GA" for "Good Articles")
        @type assessmentClass: unicode
        """

        listRe = re.compile("List[ _]of");
        disambigRe = re.compile("\(disambiguation\)");

        startCat = "Wikipedia_1.0_assessments";

        classMatch = "{assessmentClass}-Class%articles".format(
            assessmentClass=assessmentClass);

        # Query to grab the titles of all sub-categories matching
        # a given assessment class pattern from the starting category.
        getSubCatsQuery = ur'''SELECT DISTINCT(p.page_id)
                               FROM categorylinks cl
                               JOIN page p ON cl.cl_from=p.page_id
                               WHERE p.page_namespace=14
                               AND p.page_title LIKE %(classmatch)s
                               AND cl_to IN (SELECT p.page_title
                                             FROM categorylinks cl
                                             JOIN page p
                                             ON cl.cl_from=p.page_id
                                             WHERE p.page_namespace=14
                                             AND cl.cl_to=%(startcat)s
                                             AND p.page_title LIKE "%%by_quality")''';
        
        # Query to grab sub-categories of a given set of categories where
        # the category title matches a given pattern
        validSubCatQuery = ur'''SELECT p.page_id, p.page_is_redirect
                                FROM categorylinks cl
                                JOIN page p
                                ON cl.cl_from=p.page_id
                                WHERE p.page_namespace=14
                                AND p.page_title LIKE "{classmatch}"
                                AND cl.cl_to IN (
                                    SELECT page_title
                                    FROM page
                                    WHERE page_id IN ({pageidlist}))''';

        # Query to get all pages from a given set of categories
        getArticlesQuery = ur'''SELECT p2.page_title, p2.page_id, p2.page_is_redirect
                                FROM page p2
                                JOIN page p1
                                ON p1.page_title=p2.page_title
                                JOIN categorylinks cl
                                ON cl.cl_from=p1.page_id
                                WHERE p2.page_namespace={ns}
                                AND cl.cl_to IN (
                                   SELECT page_title
                                   FROM page
                                   WHERE page_id IN ({pageidlist}))''';

        # Query to resolve redirects that go to a given namespace
        resolveRedirectQuery = ur"""SELECT page_title, page_id, page_is_redirect
                                    FROM redirect
                                    JOIN page
                                    ON (rd_namespace=page_namespace
                                    AND rd_title=page_title)
                                    WHERE rd_from IN ({pageidlist})
                                    AND page_namespace={ns}""";

        getPagesFromCategoryQuery = ur"""SELECT cat_pages
                                         FROM category
                                         WHERE cat_id IN ({pageidlist})""";

        logging.info("Getting {aClass}-Class articles".format(aClass=assessmentClass));

        # Find all matching sub-categories
        allSubCats = []
        self.dbCursor.execute(getSubCatsQuery,
                              {'startcat': startCat,
                               'classmatch': classMatch});
        for row in self.dbCursor.fetchall(): # not too many, fetchall's ok
            allSubCats.append(str(row['page_id'])); # str() for join() later

        logging.info("Found {n} subcategories to grab articles from".format(n=len(allSubCats)));

        # Do an exhaustive search in the sub-categories for valid child categories.
        candidateCats = list(allSubCats);
        seenCats = set();
        moreSubCats = set();

        logging.info("Looking for sub*-categories...");

        i = 0;
        sliceSize = 100;
        while i < len(candidateCats):
            logging.info("Have {n} candidate categories, taking slice {j}:{k}".format(n=len(candidateCats), j=i, k=i+sliceSize));

            curSlice = candidateCats[i:i+sliceSize];
            seenCats.update(curSlice);

            redirects = [];

            self.dbCursor.execute(validSubCatQuery.format(pageidlist=",".join(candidateCats[i:i+sliceSize]), classmatch=classMatch));
            for row in self.dbCursor.fetchall():
                pageId = str(row['page_id']);
                if not pageId in seenCats:
                    seenCats.add(pageId);
                    if row['page_is_redirect']:
                        # add to redirects to check
                        redirects.append(pageId);
                    else:
                        # Valid category, add to candidate for further inspection
                        # and to list of categories to fetch articles from
                        moreSubCats.add(pageId);
                        candidateCats.append(pageId);

            # Resolve redirects
            if redirects:
                logging.info("Resolving {n} redirects".format(n=len(redirects)));

                j = 0;
                while j < len(redirects):
                    self.dbCursor.execute(resolveRedirectQuery.format(ns=14,
                                                                      pageidlist=",".join(redirects[j:j+sliceSize])));
                    for row in self.dbCursor.fetchall():
                        if not row['page_is_redirect']:
                            pageId = str(row['page_id']);
                            if not row in seenCats:
                                # Valid category, add to candidate for further inspection
                                # and to list of categories to fetch articles from
                                seenCats.add(pageId);
                                moreSubCats.add(pageId);
                                candidateCats.append(pageId);

                    # OK, move redirects forward
                    j += sliceSize

            # OK, move categories forward
            i += sliceSize;

        logging.info("Found {n} valid sub*-categories, unionising with the other sub-categories".format(n=len(moreSubCats)));

        allSubCats = set(allSubCats) | moreSubCats;
        allSubCats = [pageid for pageid in allSubCats]; # listify for slicing

        logging.info("Now have {n} categories to grab articles from".format(n=len(allSubCats)));

        # Grab all articles from them, resolving redirects as necessary
        allArticles = set();
        redirects = set();

        i = 0;
        sliceSize = 100;
        while i < len(allSubCats):
            self.dbCursor.execute(getArticlesQuery.format(pageidlist=",".join(allSubCats[i:i+sliceSize]), ns=0));
            for row in self.dbCursor.fetchall():
                # List or disambiguation? Then skip...
                pageTitle = unicode(row['page_title'], 'utf-8', errors='strict')
                if listRe.match(pageTitle) \
                        or disambigRe.search(pageTitle):
                    logging.info(u"Ignoring list or disambig: {title}".format(title=pageTitle))
                    continue

                if row['page_is_redirect']:
                    redirects.add(row['page_id'])
                else:
                    allArticles.add(row['page_id'])

            i += sliceSize;
                    
        logging.info("Found {n} articles and {m} redirects.".format(n=len(allArticles),
                                                                    m=len(redirects)));

        # resolve single redirects
        i = 0;
        sliceSize = 100;
        redirects = [str(pageid) for pageid in redirects];
        while i < len(redirects):
            self.dbCursor.execute(resolveRedirectQuery.format(pageidlist=",".join(redirects[i:i+sliceSize]), ns=0));
            for row in self.dbCursor.fetchall():
                # List or disambiguation? Then skip...
                pageTitle = unicode(row['page_title'], 'utf-8', errors='strict');
                if listRe.match(pageTitle) \
                        or disambigRe.search(pageTitle):
                    continue;

                if not row['page_is_redirect']:
                    allArticles.add(row['page_id']);

            i += sliceSize;

        logging.info("Found {n} articles in total".format(n=len(allArticles)));

        # logging.info("Checking article count using the category table");

        # articleCount = 0;
        # sliceSize = 100;
        # i = 0;
        # while i < len(allSubCats):
        #     self.dbCursor.execute(getPagesFromCategoryQuery.format(pageidlist=",".join(allSubCats[i:i+sliceSize])));
        #     for row in self.dbCursor.fetchall():
        #         articleCount += row['cat_pages'];

        #     i += sliceSize;

        # logging.info("Total page count from category table: {n}".format(n=articleCount));

        return allArticles;

    def getAClassArticles(self):
        """
        Get a count of all articles in all the categories like "A-Class%articles".
        """

        getCatQuery = ur"""SELECT DISTINCT(page_id)
                           FROM categorylinks
                           JOIN page
                           ON cl_from=page_id
                           WHERE cl_to LIKE 'A-Class%articles'
                           AND page_namespace=14""";

        getPagesQuery = ur"""SELECT p.page_id
                             FROM categorylinks cl
                             JOIN page p
                             ON cl.cl_from=p.page_id
                             WHERE p.page_namespace=0
                             AND cl.cl_to IN (
                                 SELECT p2.page_title
                                 FROM page p2
                                 WHERE p2.page_id IN ({pageidlist}))""";

        getPagesFromCategoryQuery = ur"""SELECT cat_pages
                                         FROM category
                                         WHERE cat_id IN ({pageidlist})""";

        allCats = [];

        self.dbCursor.execute(getCatQuery);
        for row in self.dbCursor.fetchall():
            allCats.append(str(row['page_id']));

        logging.info("Got {n} categories like 'A-class%articles'".format(n=len(allCats)));

        allArticles = set();

        sliceSize = 100;
        i = 0;
        while i < len(allCats):
            self.dbCursor.execute(getPagesQuery.format(pageidlist=",".join(allCats[i:i+sliceSize])));
            for row in self.dbCursor.fetchall():
                allArticles.add(row['page_id']);

            i += sliceSize;
            logging.info("Grabbed slice {i}:{j}, got {n} articles in total".format(i=i, j=i+sliceSize, n=len(allArticles)));

        articleCount = 0;
        sliceSize = 100;
        i = 0;
        while i < len(allCats):
            self.dbCursor.execute(getPagesFromCategoryQuery.format(pageidlist=",".join(allCats[i:i+sliceSize])));
            for row in self.dbCursor.fetchall():
                articleCount += row['cat_pages'];

            logging.info("Grabbed slice {i}:{j}, got {n} articles in total".format(i=i, j=i+sliceSize, n=articleCount));
            i += sliceSize;

        return;

    def getArticles(self, categoryName=None, matchRegex=None):
        """
        Grab all articles from the given category.  Also, traverse down all sub-categories.
        Expects the category to point to talk pages, from which the corresponding article
        will be retrieved.

        @param categoryName: name of the category we're fetching articles from
        @type categoryName: unicode

        @param matchRegex: regular expression used for matching category names,
                           sub-categories not matching the regex are not traversed
        @type matchRegex: re._sre.SRE_Pattern
        """
        
        getArticlesQuery = ur'''SELECT p2.page_id, p2.page_is_redirect
                                FROM categorylinks cl
                                JOIN page p1
                                ON cl.cl_from=p1.page_id
                                JOIN page p2 ON p1.page_title=p2.page_title
                                WHERE p2.page_namespace=0
                                AND cl_to=%(catname)s''';

        getSubCatQuery = ur"""SELECT page_id, page_title
                              FROM categorylinks cl
                              JOIN page p
                              ON cl.cl_from=p.page_id
                              WHERE p.page_namespace=14
                              AND cl.cl_to=%(catname)s""";

        # Query to resolve redirects that stay within the Main namespace
        resolveRedirectQuery = ur"""SELECT page_id, page_is_redirect
                                    FROM redirect
                                    JOIN page
                                    ON (rd_namespace=page_namespace
                                    AND rd_title=page_title)
                                    WHERE rd_from IN ({pageidlist})
                                    AND page_namespace=0""";

        # for easy FIFO queues, we use deque;
        from collections import deque;

        foundArticles = set();

        # sub any spaces with underscores for queries
        catName = re.sub(" ", "_", categoryName);

        # find all articles
        redirects = [];

        self.dbCursor.execute(getArticlesQuery,
                            {'catname': catName.encode('utf-8')});
        done = False;
        while not done:
            row = self.dbCursor.fetchone();
            if not row:
                done = True;
                continue;

            if row['page_is_redirect']:
                redirects.append(str(row['page_id'])); # str() for easy join later
            else:
                foundArticles.add(row['page_id']);

        self.seenCount += len(foundArticles);

        # logging.info("Found {n} articles".format(n=len(foundArticles)));
        # logging.info("Attempting to resolve {n} redirects".format(n=len(redirects)));

        # resolve single redirects
        i = 0;
        sliceSize = 100;
        while i < len(redirects):
            self.dbCursor.execute(resolveRedirectQuery.format(pageidlist=",".join(redirects[i:i+sliceSize])));
            for row in self.dbCursor.fetchall():
                if not row['page_is_redirect']:
                    foundArticles.add(row['page_id']);

            i += sliceSize;

        redirects = None; # no longer needed

        # logging.info("Resolved redirects, found {n} articles so far".format(n=len(foundArticles)));

        seenCats = set(); # seen categories

        # simple FIFO queue of categories we'll be grabbing articles from,
        # initialised with the current category name
        catQueue = deque([catName]);

        while len(catQueue) > 0:
            # grab the current category
            curCategory = catQueue.popleft();

            # sub any spaces with underscores for queries
            catName = re.sub(" ", "_", curCategory);

            # find all articles
            redirects = [];
            self.dbCursor.execute(getArticlesQuery,
                                  {'catname': catName.encode('utf-8')});
            done = False;
            while not done:
                row = self.dbCursor.fetchone();
                if not row:
                    done = True;
                    continue;
                
                if row['page_is_redirect']:
                    redirects.append(str(row['page_id'])); # str() for easy join later
                else:
                    foundArticles.add(row['page_id']);

            # resolve single redirects
            i = 0;
            sliceSize = 100;
            while i < len(redirects):
                self.dbCursor.execute(resolveRedirectQuery.format(pageidlist=",".join(redirects[i:i+sliceSize])));
                for row in self.dbCursor.fetchall():
                    if not row['page_is_redirect']:
                        foundArticles.add(row['page_id']);

                i += sliceSize;

            redirects = None; # no longer needed

            # find all sub-categories
            self.dbCursor.execute(getSubCatQuery,
                                  {'catname': catName.encode('utf-8')});
            for row in self.dbCursor.fetchall():
                subCatName = unicode(row['page_title'], 'utf-8', errors='strict');
                if re.match(matchRegex, subCatName) \
                        and not subCatName in seenCats:
                    catQueue.append(subCatName);
                    seenCats.add(subCatName);

            logging.info("Found {n} articles, category queue length is {k}".format(n=len(foundArticles), k=len(catQueue)));

        return foundArticles;

    def sample(self):
        """
        Sample articles using the given configuration.
        """
        
        # Based on the number of articles and such, it appears better to use
        # some type of traversal strategy to find as many articles as possible
        # and randomise them in-memory.  If we use WPBot 1.0's categories as
        # a starting point, we'll need traversal to make it work.

        # sort the categories based on 'sortkey'
        sortedCats = sorted(self.catConfig, key=lambda cat: cat['sortkey'])

        # FIXME: all articles that are collected from a given class
        # need to be added to the self.alreadySampled set.  If we do that
        # we'll only allow an article to be sampled from the _highest_ class
        # it is assessed as.  We could also only use articles that are claimed
        # to fit into one class, but I think that pushes the assessment lag problem.

        # for each category...
        for catData in sortedCats:
            # grab all articles
            classArticles = self.getAssessmentClassArticles(assessmentClass=catData['classname']);
            # take out any articles that have already been selected
            classArticles -= self.alreadySampled;

            # Add the remaining articles to the set of already seen articles.
            # This is done to assure that an article is only sampled from
            # the _highest_ assessment it might have.  Otherwise we could
            # first have an article as an A-class candidate, then later have
            # it as a B-class candidate, because we'd see it again.
            self.alreadySampled |= classArticles;

            # listify for selection
            classArticles = list(classArticles);
            # randomise
            random.shuffle(classArticles);
            # do we have enough articles?
            k = catData['narticles'];

            if self.sampleTestSet and len(classArticles) < 2*k:
                logging.warning("Cannot sample {k} articles for training and test sets, only {n} available, using n/2".format(k=k, n=len(classArticles)));
                k = len(classArticles) / 2; # int/int division, rounds down
            elif len(classArticles) < k:
                logging.warning("Cannot sample {k} articles, only {n} available, using those".format(k=k, n=len(classArticles)));
                k = len(classArticles);
                                          
            # Store the samples and update lists of known articles
            catData['dataset'] = classArticles[:k];
            self.alreadySampled.update(catData['dataset']);
            if self.sampleTestSet:
                catData['testset'] = classArticles[k:(2*k)];
                self.alreadySampled.update(catData['testset']);

            logging.info("Sampled {k} articles for this category".format(k=k));

        logging.info("Done sampling, writing dataset");

        # Now that we have data, write output
        with codecs.open(os.path.expanduser(self.outputFilename), 'w+', 'utf-8') as outFile:
            outFile.write("classname\tpageid\n"); # write header
            for catData in sortedCats:
                for pageId in catData['dataset']:
                    outFile.write("{classname}\t{pageid}\n".format(classname=catData['classname'], pageid=pageId));

        # Write test-set too?
        if self.sampleTestSet:
            logging.info("Writing test dataset as well");
            outputFilename = "{basename}.testset".format(basename=self.outputFilename);
            with codecs.open(os.path.expanduser(outputFilename), 'w+', 'utf-8') as outFile:
                outFile.write("classname\tpageid\n"); # write header
                for catData in sortedCats:
                    for pageId in catData['testset']:
                        outFile.write("{classname}\t{pageid}\n".format(classname=catData['classname'], pageid=pageId));
            
        logging.info("All done!");

        return;

def main():
    import argparse;
    
    cli_parser = argparse.ArgumentParser(
        description="Program to sample articles in assessment classes for quality prediction."
        );

    cli_parser.add_argument("-v", "--verbose", action="store_true",
                            help="write informational output");

    cli_parser.add_argument("-t", "--testset", action="store_true",
                            help="also sample a testset of equal size");

    cli_parser.add_argument("-c", "--configfile", metavar="<config-path>",
                            default=None,
                            help="path to config file (default: sample-setup.txt)");

    cli_parser.add_argument("-o", "--outputfile", metavar="<output-path>",
                            default=None,
                            help="path to output file (default: sample-assessment-articles)");

    args = cli_parser.parse_args();

    if args.verbose:
        logging.basicConfig(level=logging.DEBUG);

    mySampler = ArticleSampler(sampleConfigFile=args.configfile,
                               outputFilename=args.outputfile,
                               sampleTestSet=args.testset);
    if not mySampler.connect():
        logging.error("Couldn't connect to database server, unable to continue");
        return;

    mySampler.sample();

    mySampler.disconnect();

    # ok, done
    return;

if __name__ == "__main__":
    main();
