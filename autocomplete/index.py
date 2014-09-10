#-*- coding:utf-8 -*-
import redis
import simplejson
#import jieba
import re

class Autocomplete (object):
  """
  autocomplete.
  """

  def __init__ (self, scope, redisaddr="localhost", limits=5, cached=True):
    self.r = redis.Redis (redisaddr)
    self.scope = scope
    self.cached=cached
    self.limits = limits
    self.database = "database:%s" % scope
    self.indexbase = "indexbase:%s" % scope

  def _get_index_key (self, key):
    return "%s:%s" % (self.indexbase, key)

  def del_index (self):
    prefixs = self.r.smembers (self.indexbase)
    for prefix in prefixs:
      self.r.delete(self._get_index_key(prefix))
    self.r.delete(self.indexbase)
    self.r.delete(self.database)

  def sanity_check (self, item):
    """
    Make sure item has key that's needed.
    """
    for key in ("uid","term"):
      if not item.has_key (key):
        raise Exception ("Item should have key %s"%key )

  def add_item (self,item):
    """
    Create index for ITEM.
    """
    self.sanity_check (item)
    self.r.hset (self.database, item.get('uid'), simplejson.dumps(item))
    for prefix in self.prefixs_for_term (item['term']):
      self.r.sadd (self.indexbase, prefix)
      self.r.zadd (self._get_index_key(prefix),item.get('uid'), item.get('score',0))

  def del_item (self,item):
    """
    Delete ITEM from the index
    """
    for prefix in self.prefixs_for_term (item['term']):
      self.r.zrem (self._get_index_key(prefix), item.get('uid'))
      if not self.r.zcard (self._get_index_key(prefix)):
        self.r.delete (self._get_index_key(prefix))
        self.r.srem (self.indexbase, prefix)

  def update_item (self, item):
    self.del_item (item)
    self.add_item (item)

  def prefixs_for_term (self,term):
    """
    Get prefixs for TERM.
    """
    # Normalization
    term=term.lower()

    # Prefixs for term
    prefixs=[]
	#tokens = jieba.cut(term)
    tokens = re.split(r'\s+', term)
    for token in tokens:
      for i in xrange (1,len(token)+1):
        prefixs.append(token[:i])

    return prefixs

  def normalize (self,prefix):
    """
    Normalize the search string.
    """
	#return [token for token in jieba.cut(prefix.lower())]
    return [token for token in re.split(r'\s+', prefix.lower())]

  def search_query (self,prefix):
    search_strings = self.normalize (prefix)

    if not search_strings: return []

    cache_key = self._get_index_key (('|').join(search_strings))

    if not self.cached or not self.r.exists (cache_key):
      self.r.zinterstore (cache_key, map (lambda x: self._get_index_key(x), search_strings))
      self.r.expire (cache_key, 10 * 60)

    ids=self.r.zrevrange (cache_key, 0, self.limits)
    if not ids: return ids
    return map(lambda x:simplejson.loads(x),
               self.r.hmget(self.database, *ids))
