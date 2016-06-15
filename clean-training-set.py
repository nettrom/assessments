#!/usr/env/python
# -*- coding: utf-8 -*-
'''
Script that takes the training set of articlers for our quality prediction
classifier and figures out the earliest revision that was assessed as the
given class used in our dataset.  Then writes the revision ID into
a new dataset.
'''

import db

import re
import sys
import logging
import codecs

import pywikibot
import mwparserfromhell as mwp

from collections import namedtuple

import MySQLdb

from assessment import Assessment
import revisions

class TPRevision:
    def __init__(self, id, timestamp, content=None):
        self.id = id
        self.timestamp = timestamp
        self.content = content

class AssessmentFinder:
    def __init__(self, is_training=False):
        '''
        Instantiate finder.

        @param is_training: are we getting clean data for the training set?
                           (if true, we move backwards in time, else forward)
        @type is_training: bool
        '''

        self.is_training = is_training

        (self.dbconn, self.dbcursor) = db.connect()
        self.db_attempts = 3 # number of query attempts

        self.site = pywikibot.Site('en')
        self.site.login()

        # Translations of known templates
        self.translations = {u'maths rating': u'wikiproject mathematics'}

    def get_assessments(self, rev_content):
        '''
        For the given revision content, get all assessments.

        @param rev_content: wikitext content of the given talk page revision
                            we're assessing
        @type rev_content: unicode
        '''

        parsed_code = mwp.parse(rev_content)
        templates = parsed_code.filter_templates()
        assessments = []
        for temp in templates:
            if re.match('wikiproject\s+',
                        unicode(temp.name),
                        re.I) \
                or unicode(temp.name) in self.translations \
                or temp.has_param('class'):
                project = unicode(temp.name).lower()
                try:
                    rating = unicode(temp.get('class').value).strip().lower()
                except ValueError:
                    continue # no assessment class in template
                importance = None
                if temp.has_param('importance'):
                    importance = unicode(temp.get('importance').value).strip().lower()
                assessments.append(Assessment(rating,
                                              importance,
                                              project))
        # return all assessments
        return assessments

    def clean_training_set(self, dataset_filename, output_filename):
        '''
        Clean the given training set by checking for older revisions
        with the same assessment rating, to find the one that was actually
        assessed as being a given class.

        @param dataset_filename: name of the input dataset we want to clean
        @type dataset_filename: str

        @param output_filename: path to write output file w/clean data
        @type output_filename: str
        '''

        articles = []

        # read in dataset, store page ID, talk page ID,
        # revision ID, training class

        with codecs.open(dataset_filename, 'r', 'utf-8') as infile:
            infile.readline() # skip header
            for line in infile:
                cols = line.strip().split()
                articles.append({'revid': None,
                                 'pageid': cols[1],
                                 'talkpageid': None,
                                 'talkpagerev': None,
                                 'class': cols[0]})

        print('Got dataset with {n} articles'.format(n=len(articles)))
        # write out new dataset
        with codecs.open(output_filename, 'w+', 'utf-8') as outfile:
            outfile.write(u'pageid\trevid\ttalkpageid\ttalkpagerevid\tclass\n')

            i = 0
            for article in articles:
                self.clean_article(article)
                outfile.write(u'{pageid}\t{revid}\t{talkpageid}\t{talkpagerev}\t{class}\n'.format(**article))
                i += 1
                if i % 500 == 0:
                    print('Written {0} articles to {1}'.format(i, output_filename))
                    sys.stdout.flush()

        return

    def get_recent_assessments(self, revid):
        '''
        Get all assessments for the article with the given revision ID.

        Assumes a working database connection is established.

        @param revid: revision ID of the page we're examining
        @type revid: long
        '''
        
        # find the page title and timestamp of the given revision
        tt_query = ur'''SELECT rev_timestamp, page_title
                        FROM revision JOIN page on rev_page=page_id
                        WHERE rev_id = %(revid)s'''

        # find the most recent revision of the talk page before the given time
        talkrev_query = ur'''SELECT rev_id
                             FROM page JOIN revision
                             ON page_id=rev_page
                             WHERE page_namespace=1
                             AND page_title=%(title)s
                             AND rev_timestamp < %(timestamp)s
                             ORDER BY rev_timestamp DESC
                             LIMIT 1'''

        if not revid:
            logging.error('Cannot find assessments without an article revid')
            return []

        rev_timestamp = None
        page_title = None
        self.dbcursor.execute(tt_query,
                              {'revid': revid})
        for row in self.dbcursor:
            rev_timestamp = int(row['rev_timestamp'])
            page_title = unicode(row['page_title'],
                                 'utf-8',
                                 errors='strict')

        if not rev_timestamp:
            logging.warning('failed to get timestamp for article rev ID {0}'.format(revid))
            return []

        tp_revid = None
        self.dbcursor.execute(talkrev_query,
                              {'title': page_title.encode('utf-8'),
                               'timestamp': rev_timestamp})
        for row in self.dbcursor:
            tp_revid = int(row['rev_id'])

        if not tp_revid:
            logging.warning('did not find any prior talk page revisions for article rev ID {0}'.format(revid))
            return []

        # get the wikitext of the page
        page = pywikibot.Page(self.site,
                              u'{ns}:{title}'.format(ns=self.site.namespace(1),
                                                     title=page_title))

        return self.get_assessments(page, tp_revid)

    def is_reverted(self, revid, radius=15):
        '''
        Check if the given revision ID was reverted by the next 15 edits.
        
        @param revid: revision ID we're testing
        @type revid: int
        '''

        # get the page ID and timestamp of the current revision
        cur_query = ur'''SELECT rev_timestamp, rev_page
                         FROM revision
                         WHERE rev_id=%(revid)s'''

        # get checksums of the past 15 revisions
        past_query = ur'''SELECT rev_sha1
                          FROM revision
                          WHERE rev_page=%(pageid)s
                          AND rev_timestamp < %(timestamp)s
                          ORDER BY rev_timestamp DESC
                          LIMIT %(k)s'''

        # get checksums of the future 15 revisions
        fut_query = ur'''SELECT rev_sha1
                         FROM revision
                         WHERE rev_page=%(pageid)s
                         AND rev_timestamp > %(timestamp)s
                         ORDER BY rev_timestamp ASC
                         LIMIT %(k)s'''

        attempts = 0
        pageid = None
        timestamp = None
        prev_checksums = set()
        future_checksums = list()

        while attempts < self.db_attempts:
            try:
                self.dbcursor.execute(cur_query,
                                      {'revid': revid})
                for row in self.dbcursor:
                    pageid = row['rev_page']
                    timestamp = row['rev_timestamp']
                    
                if not pageid:
                    logging.warning('failed to retrieve page ID for revision ID {0}'.format(revid))
                    break

                self.dbcursor.execute(past_query,
                                      {'pageid': pageid,
                                       'timestamp': timestamp,
                                       'k': radius})
                for row in self.dbcursor:
                    prev_checksums.add(row['rev_sha1'])

                self.dbcursor.execute(fut_query,
                                      {'pageid': pageid,
                                       'timestamp': timestamp,
                                       'k': radius})
                for row in self.dbcursor:
                    future_checksums.append(row['rev_sha1'])
            except MySQLdb.OperationalError as e:
                attempts += 1
                logging.error('unable to execute revert test queries')
                logging.error('MySQLdb error {0}:{1}'.format(e.args[0], e.args[1]))
                db.disconnect(self.dbconn, self.dbcursor)
                (self.dbconn, self.dbcursor) = db.connect()
            else:
                break # ok, done

        if attempts >= self.db_attempts:
            logging.error('exhausted query attempts, aborting')
            return

        # walk through future revisions and see if they reverted
        # to one previous to the current
        i = 0
        while i < len(future_checksums):
            if future_checksums[i] in prev_checksums:
                return True
            i += 1
            
        return False
        
    def clean_article(self, articledata):
        '''
        Using info about a specific article, find out when a given class
        assessment was posted to the talk page, as well as the most recent
        article revision at that time, then fetch quality features for that
        article revision and store it in articledata.
        '''

        # Query to get talk page ID and most recent revision of
        # an article and its talk page.
        latest_query = ur''' SELECT ap.page_id AS art_id,
                             tp.page_id AS talk_id,
                             ap.page_latest AS art_latest,
                             tp.page_latest AS talk_latest
                             FROM page ap
                             JOIN page tp
                             USING (page_title)
                             WHERE tp.page_namespace=1
                             AND ap.page_id=%(pageid)s'''

        # Query to get a list of revisions for a given talk page
        # based on the timestamp of a given article revision.
        tp_revquery = ur'''SELECT rev_id, rev_timestamp
                           FROM revision
                           WHERE rev_page=%(talkpageid)s
                           AND rev_timestamp < (SELECT rev_timestamp
                           FROM revision
                           WHERE rev_id=%(revid)s)
                           ORDER BY rev_timestamp DESC'''

        # Query to get the most recent revision ID of an article
        # at the given time, based on a talk page revision ID
        recent_revquery = ur'''SELECT rev_id, rev_timestamp
                               FROM revision
                               WHERE rev_page=%(pageid)s
                               AND rev_timestamp <
                                  (SELECT rev_timestamp
                                   FROM revision
                                   WHERE rev_id=%(tp_revid)s)
                               ORDER BY rev_timestamp DESC
                               LIMIT 1'''

        # Query to get the first next revision ID of an article,
        # based on a talk page revision ID
        next_revquery = ur'''SELECT rev_id, rev_timestamp
                             FROM revision
                             WHERE rev_page=%(pageid)s
                             AND rev_timestamp >
                                (SELECT rev_timestamp
                                 FROM revision
                                 WHERE rev_id=%(tp_revid)s)
                             ORDER BY rev_timestamp ASC
                             LIMIT 1'''

        wp10_scale = {'stub': 0,
                      'start': 1,
                      'c': 2,
                      'b': 3,
                      'ga': 4,
                      'a': 5,
                      'fa': 6}

        # map the current class to a number
        start_idx = wp10_scale[articledata['class'].lower()]
        
        logging.info('initial assessment class is {0}'.format(articledata['class']))

        try:
            self.dbconn.ping()
        except:
            db.disconnect(self.dbconn, self.dbcursor)
            (self.dbconn, self.dbcursor) = db.connect()

        # Fetch talk page ID, as well as latest revision
        # of both article and talk page
        attempts = 0
        while attempts < self.db_attempts:
            try:
                
                self.dbcursor.execute(latest_query,
                                      {'pageid': articledata['pageid']})
                for row in self.dbcursor:
                    articledata['revid'] = row['art_latest']
                    articledata['talkpageid'] = row['talk_id']
                    articledata['talkpagerev'] = row['talk_latest']
            except MySQLdb.OperationalError as e:
                attempts += 1
                logging.error('unable to execute query to get talk page ID and ltest revision IDs')
                logging.error('MySQLdb error {0}:{1}'.format(e.args[0], e.args[1]))
                # reconnect
                db.disconnect(self.dbconn, self.dbcursor)
                (self.dbconn, self.dbcursor) = db.connect()
            else:
                break # ok, done

        if attempts >= self.db_attempts:
            logging.error('exhausted query attempts, aborting')
            return

        # get a list of talk page revisions after a given date
        tp_revs = []
        attempts = 0
        while attempts < self.db_attempts:
            try:
                
                self.dbcursor.execute(tp_revquery,
                                      {'talkpageid': articledata['talkpageid'],
                                       'revid': articledata['revid']})
                for row in self.dbcursor:
                    tp_revs.append(TPRevision(row['rev_id'],
                                              row['rev_timestamp']))
                logging.info('found {0} talk page revisions to inspect'.format(len(tp_revs)))
            except MySQLdb.OperationalError as e:
                attempts += 1
                logging.error('unable to execute query to get talk page revisions')
                logging.error('MySQLdb error {0}:{1}'.format(e.args[0], e.args[1]))
                # reconnect
                db.disconnect(self.dbconn, self.dbcursor)
                (self.dbconn, self.dbcursor) = db.connect()
            else:
                break # ok, done

        if attempts >= self.db_attempts:
            logging.error('exhausted query attempts, aborting')
            return

        # If it's empty it means we have the most recent revision,
        # so we can just keep the data we have and return.
        if not tp_revs:
            return

        prev_tprevid = -1
        i = 0
        slice_size = 20
        done = False
        while i < len(tp_revs) and not done:
            rev_subset = tp_revs[i:i+slice_size]
            revisions.get_revisions(self.site, rev_subset)

            for revision in rev_subset:
                logging.info('assessing talk page revision ID {0}'.format(revision.id))
                # NOTE: The assessments are at the top of the page,
                # and the templates are rather small,
                # so if the page is > 8k, truncate.
                if not revision.content:
                    logging.info('revision has no content, skipping')
                    continue

                if len(revision.content) > 8*1024:
                    logging.info('revision is {0} bytes, truncating to 8k'.format(len(revision.content)))
                    revision.content = revision.content[:8*1024]
                assessments = self.get_assessments(revision.content)
                cur_idx = []
                for assessment in assessments:
                    try:
                        cur_idx.append(wp10_scale[assessment.rating])
                    except KeyError:
                        continue # not a valid assessment

                if not cur_idx:
                    logging.info('found no assessments in this revision')
                    if self.is_reverted(revision.id):
                        logging.info('revision got reverted, continuing...')
                        continue
                    else:
                        # We have found a revision with no assessments
                        # and it was not reverted, prev_tprevid is the
                        # talk page revision ID we want to use
                        done = True
                        break

                cur_idx = max(cur_idx)
                logging.info('found assessment with class index {0}'.format(cur_idx))
                # If we have the same assessment rating
                # update prev_tprevid because
                # we then know we have a more recent assessment.
                if cur_idx == start_idx:
                    prev_tprevid = revision.id
                elif cur_idx != start_idx:
                    # We have found a revision with a lower or higher rating,
                    # that means prev_tprevid is the talk page revision ID
                    # we want to use to find the most recent article revision
                    done = True
                    break
            
            i += slice_size

        # If prev_tprevid is -1, our existing revision is the valid one
        if prev_tprevid < 0:
            return

        # Update articledata with the found talk page revision ID
        articledata['talkpagerev'] = prev_tprevid

        # Find the most recent revision of the article at the time
        # of the previous talk page revision ID.
        article_revision = None
        article_timestamp = None
        attempts = 0
        while attempts < self.db_attempts:
            try:
                self.dbcursor.execute(recent_revquery,
                                      {'pageid': articledata['pageid'],
                                       'tp_revid': prev_tprevid})
                for row in self.dbcursor:
                    article_revision = row['rev_id']
            except MySQLdb.OperationalError as e:
                attempts += 1
                logging.error('unable to execute query to get talk page revisions')
                logging.error('MySQLdb error {0}:{1}'.format(e.args[0], e.args[1]))
                db.disconnect(self.dbconn, self.dbcursor)
                (self.dbconn, self.dbcursor) = db.connect()
            else:
                break # ok, done

        if attempts >= self.db_attempts:
            logging.error('exhausted query attempts, aborting')
            return

        # error check
        if not article_revision:
            # likely a talk page created just before the article page,
            # get the first one after instead
            logging.warning('failed to get article revision for talk page revision ID {0}, picking first after instead'.format(prev_tprevid))
            attempts = 0
            while attempts < self.db_attempts:
                try:
                    self.dbcursor.execute(next_revquery,
                                          {'pageid': articledata['pageid'],
                                           'tp_revid': prev_tprevid})
                    for row in self.dbcursor:
                        article_revision = row['rev_id']
                except MySQLdb.Error as e:
                    attempts += 1
                    logging.error('unable to execute query to get talk page revisions')
                    logging.error('MySQLdb error {0}:{1}'.format(e.args[0], e.args[1]))
                    db.disconnect(self.dbconn, self.dbcursor)
                    (self.dbconn, self.dbcursor) = db.connect()
                else:
                    break # ok, done

        if not article_revision:
            logging.error('picking first next revision also failed, unable to continue')
            return

        logging.info('new most recent article revision ID {0}'.format(article_revision))
        # update article data with new revision ID
        articledata['revid'] = article_revision

        # all done
        return

def main():
    import argparse
    
    cli_parser = argparse.ArgumentParser(
        description="Script to clean our training dataset so it contains the most recent revision at time of assessment."
        )

    cli_parser.add_argument("-v", "--verbose", action="store_true",
                            help="write informational output");

    cli_parser.add_argument('input_file', type=str,
                            help='path to input TSV training set file')
    cli_parser.add_argument('output_file', type=str,
                            help='path to output TSV cleaned training set file')

    args = cli_parser.parse_args()
    if args.verbose:
        logging.basicConfig(level=logging.DEBUG)

    finder = AssessmentFinder()
    finder.clean_training_set(args.input_file, args.output_file)

    # ok, done
    return

if __name__ == '__main__':
    main()
