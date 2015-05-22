#!/usr/env/python
# -*- coding: utf-8 -*-
'''
Class for article assessments.
'''

class Assessment:
    def __init__(self, rating, importance=None, project=None):
        self.rating = rating
        self.importance = importance
        self.project = project

    def __str__(self):
        return u'Project: {0}, Class: {1}, Importance: {2}'.format(self.project, self.rating, self.importance)
