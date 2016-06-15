#!/usr/env/python
# -*- coding: utf-8 -*-
'''
Utility function for efficiently fetching a set of revisions from
the Wikipedia API through pywikibot.
'''

import logging
import pywikibot
from collections import namedtuple

def get_revisions(site, revisions, errorpages={}):
    '''
    Use the API for the given Wikipedia site to efficiently fetch
    revisions given a list of revision (namedtuples).

    Returns a list of named tuples with properties:
     - id: revision ID
     - content: content of the given revision

    @param site: site we're querying
    @type site: pywikibot.Site

    @param revisions: list of revision to retrieve
    @type revisions: list

    @param errorpages: dictionary storing info about pages with errors
    @type errorpages: dict
    '''

    # How many pages at a time can we process?
    slice_size = 10

    # We get in a list of revisions, but from the API we'll get
    # pages and a list of revisions.  Make a map for easy retrieval
    # where we'll store the revision content.
    revisions_map = dict((str(rev.id), rev) for rev in revisions)

    i = 0
    while i < len(revisions):
        # make an API query to get info about a subset of pages
        id_subset = revisions[i:i+slice_size]

        # This query might get truncated because we're requesting revisions
        req = pywikibot.data.api.Request(site=site,
                                         action='query')
        req['prop'] = u'info|revisions'
        req['rvprop'] = u'ids|timestamp|size|content'
        req['revids'] = "|".join(str(rev.id) for rev in id_subset)

        requestdata = {}

        query_done = False
        while not query_done:
            data = req.submit()
            requestdata.update(data)
            if 'query-continue' in data:
                # Example: {u'revisions': {u'rvcontinue': u'446891|552013814'}}
                logging.info(u'query-continue: {cont}'.format(cont=data['query-continue']))
                for contprop, contdata in data['query-continue'].iteritems():
                    for contkey, contval in contdata.iteritems():
                        req[contkey] = contval
            else:
                query_done = True

        # data.keys() = [u'query']
        # data['query'].keys() = [u'pages', u'userinfo']
        # data['query']['pages'] is a dict mapping page IDs (as strings)
        # to data for a given page

        if not 'pages' in requestdata['query']:
            logging.warning("No info about pages in API info query")
        else:
            for pageid, pagedata in requestdata['query']['pages'].iteritems():
                logging.info("Processing page ID {pageid}".format(pageid=pageid))

                if not 'revisions' in pagedata \
                   or not pagedata['revisions']:
                    logging.warning("Page {pageid} has no revisions?".format(pageid=pageid))
                    errorpages[pageid] = "No revisions?"
                    continue

                for revision in pagedata['revisions']:
                    revid = str(revision['revid'])
                    try:
                        content = revision['*']
                    except KeyError:
                        logging.warning(u'Failed to get revision text for revision {revid}'.format(revid=revid))
                        content = None
                    # store in our dictionary
                    revisions_map[revid].content = content

        # done processing, iterate
        i += slice_size

    # Loop through the list of revisions we were handed and build
    # a matching list
    result = []
    for rev in revisions:
        result.append(rev)
        
    return result
