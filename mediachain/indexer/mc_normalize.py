#!/usr/bin/env python

"""
Last-moment normalization of records.

Future:
- cache these?
- multi-pass supervised ML methods?
- refine difference between normalizer vs translator?

"""

import urlparse

def walk_json_leaves(hh, path = []):
    """yields (path, value) tuples"""
    
    path = path[:]
    for k,v in hh.iteritems():
        
        if type(v) != dict:
            yield path + [k], v
        
        if type(v) == dict:
            for xx in walk_json_leaves(v, path + [k]):
                yield xx

            
def get_shallowest_matching(hh, kk):
    """
    Walk `hh` and return value of shallowest truthy leaf whose immediate-parent key matches string `kk`.
    Ex: get_shallowest_matching({'1':{'2':{'title':'4','5':{'title':'7'}}}}, 'title')
        = '4'
    """

    ## Exact matches:

    rr = list(sorted([(len(path),path[-1],v)
                      for (path, v)
                      in walk_json_leaves(hh)
                      if kk == path[-1]
                      ]))
    if rr:
        #print ('MATCH',kk,rr[0][-1])
        return rr[0][-1]

    ## Looser matches:

    rr = list(sorted([(len(path),path[-1],v)
                      for (path, v)
                      in walk_json_leaves(hh)
                      if (kk.lower() in path[-1].lower()) and (v)
                      ]))

    if rr:
        #print ('MATCH',kk,rr[0][-1])
        return rr[0][-1]

    return None


def get_shallowest_matching_join(hh, kk):
    """
    Join into a string.
    """

    rr = get_shallowest_matching(hh, kk)

    if rr:
        if (type(rr) == list) and (isinstance(rr[0], basestring)):
            rr = '; '.join(rr)
        return rr

    return None


def print_json_by_subtree_size(h):
    #http://stackoverflow.com/questions/18871217/how-to-custom-sort-a-list-of-dict-to-use-in-json-dumps
    pass


from mc_ingest import decode_image
from PIL import Image
from cStringIO import StringIO

def get_image_stats(s):
    """
    Get stats from the thumbnail.
    """
    img = Image.open(StringIO(decode_image(s)))
    h = {}
    #print ('FORMAT',img.format)
    h['mime'] = Image.MIME[img.format]
    h['width'] = img.size[0]
    h['height'] = img.size[1]
    h['aspect_ratio'] = img.size[0] / float(img.size[1]) ## TODO: Round this off, for consistency across serializers?
    return h

def normalize_eyeem(iter_json):
    """
    // SCHEMA: eyeem:
    
    {'og:url': 'https://www.eyeem.com/p/3879', 'description': ['illuminated', 'indoors', 'night', 'reflection', 'lighting equipment', 'window', 'glass - material', 'light - natural phenomenon', 'transparent', 'defocused', 'water', 'dark,'], 'og:description': 'Photo by @BassamLahhoud', 'og:title': 'Description', 'twitter:title': 'Description', 'og:image': 'https://cdn.eyeem.com/thumb/3c5a37cc43079c4fc20c861771c2e58a1023815c.jpg/640/480'}
    """

    assert not hasattr(iter_json, 'next'), 'Should be function that returns iterator when called, to allow restarting.'

    for jj_top in iter_json():
        
        jj = jj_top
        
        assert jj['og:url'].startswith('https://www.eyeem.com/p/'),repr(jj)
        
        #native_id = 'eyeem_' + jj['og:url'].replace('https://www.eyeem.com/p/','')
        
        native_id = jj['_id']
        
        xid = make_id(native_id)
        
        artist_name = jj['og:description'].replace('Photo by @', '')
        
        assert native_id.startswith('eyeem_'),repr(native_id)
        
        ims = get_image_stats(jj_top['img_data'])
        
        sizes = [{'width':ims['width'],           # pixel width
                  'height':ims['height'],         # pixel height
                  'dpi':None,                     # DPI - Use to estimate real-world width / height if needed?
                  'bytes':None,                   # bytes size of this version
                  'content_type':ims['mime'],     # Image mime-type
                  'uri_external':None,            # External URI.
                  }
                 ]
        
        hh = {'_id':xid,
              'aspect_ratio':ims['aspect_ratio'],
              'native_id':native_id,
              'source_dataset':'eyeem',
              'source':{'name':'eyeem',
                        'url':jj['og:url']
                        },
              'source_tags':['eyeem.com'],   # For easy indexer faceting
              'license_tags':['Non-Commercial Use'], # For easy indexer faceting
              'img_data':jj_top['img_data'],       # Data URI of this version -- only for thumbnails.
              'url_shown_at':{'url':jj['og:url']},
              'url_direct':jj['og:image'],
              'artist_names':[artist_name],       # Simple way to access artist name(s).
              'title':[jj['og:title']],               # Title string(s)
              'sizes':sizes,
              'attribution':[{                     ## Full attribution details, for when "artist" isn't quite the right description.
                  'role':'artist',                 # Contribution type.
                  'details':None,                  # Contribution details.
                  'name':artist_name,             # Entity name.
                  }],
              'keywords':jj['description'],
              'license_name':"EyeEm License",
              'license_name_long':"EyeEm License",
              'license_url':"https://www.eyeem.com/market/licensing#find-your-license",
              'licenses':[                         ## List of licenses:
                  {'name':'EyeEm License',            
                   'name_long':'EyeEm ',            
                   'attribute_to':[],              # For this license, attribute to this person / organization / paper citation.
                   'details':[],                   # License details text
                   }],
              
              }

        #h2 = hh.copy()
        #del h2['img_data']
        #print 'INSERTING',h2
        #raw_input_enter()
        
        yield hh

    


def normalize_getty(iter_json):
    """
    // SCHEMA: getty:

    {
        "source_record": {
            "orientation": "Square",
            "links": [
                {
                    "uri": "https://api.gettyimages.com/v3/images/57305676/similar",
                    "rel": "similar"
                },
                {
                    "uri": "https://api.gettyimages.com/v3/search/images/artists?name=Stockbyte",
                    "rel": "artist"
                }
            ],
            "credit_line": "Stockbyte",
            "call_for_image": false,
            "alternative_ids": {},
            "keywords": [
                {
                    "relevance": null,
                    "keyword_id": "60124",
                    "type": "Unknown",
                    "text": "Drink"
                },
                {
                    "relevance": null,
                    "keyword_id": "60259",
                    "type": "Unknown",
                    "text": "Coffee Maker"
                },
                {
                    "relevance": null,
                    "keyword_id": "60558",
                    "type": "Composition",
                    "text": "Square"
                },
                {
                    "relevance": null,
                    "keyword_id": "60590",
                    "type": "Unknown",
                    "text": "Indoors"
                },
                {
                    "relevance": null,
                    "keyword_id": "61686",
                    "type": "Unknown",
                    "text": "Coffee - Drink"
                },
                {
                    "relevance": null,
                    "keyword_id": "70720",
                    "type": "ImageTechnique",
                    "text": "Color Image"
                },
                {
                    "relevance": null,
                    "keyword_id": "99907",
                    "type": "NumberOfPeople",
                    "text": "No People"
                },
                {
                    "relevance": null,
                    "keyword_id": "100604",
                    "type": "Unknown",
                    "text": "Photography"
                },
                {
                    "relevance": null,
                    "keyword_id": "116651",
                    "type": "Unknown",
                    "text": "Brightly Lit"
                },
                {
                    "relevance": null,
                    "keyword_id": "117724",
                    "type": "Entertainment",
                    "text": "White Background"
                },
                {
                    "relevance": null,
                    "keyword_id": "149872",
                    "type": "Unknown"
                },
                {
                    "relevance": null,
                    "keyword_id": "235844",
                    "type": "Unknown"
                }
            ],
            "event_ids": [],
            "id": "57305676",
            "city": "",
            "uri_oembed": "https://embed.gettyimages.com/oembed?url=http%3a%2f%2fgty.im%2f57305676&caller=17413",
            "copyright": null,
            "title": "A coffee press",
            "people": [],
            "display_sizes": [
                {
                    "width": 414,
                    "name": "comp",
                    "uri": "http://cache2.asset-cache.net/gc/57305676-coffee-press-gettyimages.jpg?v=1&c=IWSAsset&k=2&d=0wnh7UfZFIk%2bg%2fJfx%2bA%2fHLx%2bxCDjWTxOzy1wTuDWgIy2Esx1U33%2bAhgCP2VvuPJJ&b=Mzcw",
                    "is_watermarked": true,
                    "height": 414
                },
                {
                    "name": "preview",
                    "is_watermarked": true,
                    "uri": "http://cache2.asset-cache.net/gp/57305676.jpg?v=1&c=IWSAsset&k=3&d=0wnh7UfZFIk%2bg%2fJfx%2bA%2fHCbwRNgMRySZXLrZ5upQzc0%3d&b=QkZB"
                },
                {
                    "width": 170,
                    "name": "thumb",
                    "uri": "http://cache2.asset-cache.net/xt/57305676.jpg?v=1&g=fs1|0|STK|05|676&s=1&b=RTRE",
                    "is_watermarked": false,
                    "height": 170
                }
            ],
            "graphical_style": "photography",
            "product_types": [],
            "asset_family": "creative",
            "editorial_source": {
                "id": 10900,
                "name": "Stockbyte"
            },
            "color_type": "color",
            "date_submitted": "2006-04-10T23:59:20",
            "editorial_segments": [],
            "license_model": "royaltyfree",
            "date_created": "2006-04-10T16:59:21-07:00",
            "quality_rank": 3,
            "collection_id": 68,
            "prestige": false,
            "state_province": "",
            "referral_destinations": [
                {
                    "site_name": "gettyimages",
                    "uri": "http://www.gettyimages.com/detail/photo/coffee-press-royalty-free-image/57305676"
                },
                {
                    "site_name": "thinkstock",
                    "uri": "http://www.thinkstockphotos.com/image/stock-photo-a-coffee-press/57305676"
                }
            ],
            "artist": "Stockbyte",
            "collection_name": "Stockbyte",
            "artist_title": "None",
            "caption": null,
            "date_camera_shot": null,
            "country": "",
            "max_dimensions": {
                "width": 5120,
                "height": 5120
            },
            "download_sizes": [
                {
                    "media_type": "image/jpeg",
                    "bytes": 1109,
                    "width": 66,
                    "height": 66
                },
                {
                    "media_type": "image/jpeg",
                    "bytes": 4496,
                    "width": 170,
                    "height": 170
                },
                {
                    "media_type": "image/jpeg",
                    "bytes": 13463,
                    "width": 280,
                    "height": 280
                },
                {
                    "media_type": "image/jpeg",
                    "bytes": 26313,
                    "width": 414,
                    "height": 414
                },
                {
                    "media_type": "image/jpeg",
                    "bytes": 55463,
                    "width": 1024,
                    "height": 1024
                },
                {
                    "media_type": "image/jpeg",
                    "bytes": 63044,
                    "width": 592,
                    "height": 592
                },
                {
                    "media_type": "image/jpeg",
                    "bytes": 152176,
                    "width": 1025,
                    "height": 1025
                },
                {
                    "media_type": "image/jpeg",
                    "bytes": 156894,
                    "width": 2048,
                    "height": 2048
                },
                {
                    "media_type": "image/jpeg",
                    "bytes": 377095,
                    "width": 1733,
                    "height": 1733
                },
                {
                    "media_type": "image/jpeg",
                    "bytes": 1028043,
                    "width": 3157,
                    "height": 3157
                },
                {
                    "media_type": "image/jpeg",
                    "bytes": 1925835,
                    "width": 5120,
                    "height": 5120
                }
            ],
            "allowed_use": {
                "usage_restrictions": [],
                "release_info": "Property released",
                "how_can_i_use_it": "Available for all permitted uses under our |License Terms|."
            },
            "collection_code": "STK"
        },
        "description": null,
        "artist": "Stockbyte",
        "editorial_source": "Stockbyte",
        "title": "A coffee press",
        "dataset": "getty",
        "collection_name": "Stockbyte",
        "keywords": "Drink Coffee Maker Square Indoors Coffee - Drink Color Image No People Photography Brightly Lit White Background",
        "date_created": "2006-04-10T16:59:21-07:00",
        "_id": "getty_57305676"
    }

    """

    assert not hasattr(iter_json, 'next'), 'Should be function that returns iterator when called, to allow restarting.'

    for jj_top in iter_json():

        jj = jj_top['source_record'] ## Ignore the minimal normalization we did initially. Look at source.

        #print jj['download_sizes']

        sizes = [{'width':x['width'],        # pixel width
                  'height':x['height'],      # pixel height
                  'dpi':None,                     # DPI - Use to estimate real-world width / height if needed?
                  'bytes':x['bytes'],        # bytes size of this version
                  'content_type':x['media_type'],
                  'uri_external':None,            # External URI.
                  }
                 for x
                 in jj['download_sizes']
                 ]

        xid = make_id(jj_top['_id'])
        
        native_id = jj_top['_id']
        
        assert native_id.startswith('getty_'),repr(native_id)
        
        hh = {'_id':xid,
              'native_id':native_id,
              'source_dataset':'getty',
              'source':{'name':'getty',
                        #'url':'http://www.gettyimages.com/',
                        'url':'http://www.gettyimages.com/detail/photo/permalink/' + jj['id'], ## TODO - 'referral_destinations'
                        },
              'source_tags':['gettyimages.com'],   # For easy indexer faceting
              'license_tags':['Non-Commercial Use'], # For easy indexer faceting
              'img_data':jj_top['img_data'],       # Data URI of this version -- only for thumbnails.
              'url_shown_at':{'url':'http://www.gettyimages.com/detail/photo/permalink/' + jj['id']},
              'url_direct':None,
              #'url_direct_cache':{'url':make_cache_url(jj_top['_id'])},
              'artist_names':[jj['artist']],       # Simple way to access artist name(s).
              'title':[jj['title']],               # Title string(s)
              'attribution':[{                     ## Full attribution details, for when "artist" isn't quite the right description.
                  'role':'artist',                 # Contribution type.
                  'details':None,                  # Contribution details.
                  'name':jj['artist'],             # Entity name.
                  }],
              'keywords':[x['text'] for x in jj['keywords'] if 'text' in x], # Keywords
              'orientation':jj['orientation'],                # Should photo be rotated?
              #'editorial_source_name':jj['editorial_source'], #
              #'editorial_source':{                           ## TODO BROKEN- mismatching types
              #    'name':jj['editorial_source'],              #
              #    },
              'date_created_original':None,                 # Actual creation date.
              'date_created_at_source':jj['date_created'],    # Item created at data source.
              'camera_exif':{},                    # Camera Exif data
              'license_name':"Getty Embed",
              'license_name_long':"Getty Embed",
              'license_url':"http://www.gettyimages.com/Corporate/LicenseAgreements.aspx#RF",
              'licenses':[                         ## List of licenses:
                  {'name':'CC0',            
                   'name_long':'Getty Embed',            
                   'attribute_to':[],              # For this license, attribute to this person / organization / paper citation.
                   'details':[],                   # License details text
                   }],
              'sizes':sizes,
              'location':{
                  'lat_lon':None,        ## Latitude / Longitude.
                  'place_name':[],       # Place Name
                  },
              'derived_qualities':{      ## Possibly derived from image / from other metadata:
                  'general_type':None,   # (photo, illustration, GIF, face)
                  'colors':None,         # Dominant colors.
                  'has_people':None,     # Has people?
                  'time_period':None,    # (contemporary, 1960s, etc)
                  'medium':jj['graphical_style'],# (photograph, drawing, vector, etc)
                  'predicted_tags':None, #
                  },
              'transient_info':{         ## Information that may frequently change.
                  'score_hotness'        # Popularity / newness
                  'views':None,          #
                  'likes':None,          #
                  },
              }

        yield hh

import hashlib
from os.path import exists, join

def make_id(_id):
    return hashlib.md5(_id).hexdigest()

def make_cache_url(_id):
    assert False,'MOVED TO mc_web.py'
    
    CACHE_HOST = 'http://54.209.175.109:6008'


    #print ('make_cache_url',_id)

    if _id.startswith('pexels_'):

        _id = _id.split('_')[-1]
        
        xid = hashlib.md5(_id).hexdigest()

        fn = ('/'.join(xid[:4])) + '/' + xid + '.jpg'

        #normalizer_names['pexels']['dir_cache']
        
        real_fn = '/datasets/datasets/pexels/images_1920_1280/' + fn

        #assert exists(real_fn),real_fn
        
        #if not exists(real_fn):
        #    print 'SKIPPING',real_fn
        #    return None
        #else:
        #    print 'FOUND',real_fn

        return CACHE_HOST + '/' + fn
    
    else:
        assert False,repr(_id)


def normalize_pexels(iter_json):
    """
    // SCHEMA: pexels:

    {
        "source_record": {
            "the_canon": "https://www.pexels.com/photo/mountain-ranges-covered-with-snow-during-daytime-24691/",
            "_id": "24691",
            "the_kw": [
                "cold",
                "snow",
                "mountains",
                "sky",
                "sun",
                "winter",
                "mountain range"
            ],
            "custom_url": "https://pexels.imgix.net/photos/24691/pexels-photo-24691.jpg",
            "img_url": "https://static.pexels.com/photos/24691/pexels-photo-24691-landscape.jpg"
        },
        "title": "cold, snow, mountains, sky, sun, winter, mountain range",
        "dataset": "pexels",
        "licenses": [
            {
                "name": "CC0",
                "description": "Creative Commons Zero (CC0)"
            }
        ],
        "_id": "pexels_24691"
    }
    """

    assert not hasattr(iter_json, 'next'), 'Should be function that returns iterator when called, to allow restarting.'

    for jj_top in iter_json():

        jj = jj_top['source_record'] ## Ignore the minimal normalization we did initially. Look at source.

        ims = get_image_stats(jj_top['img_data'])
        
        sizes = [{'width':1920,                   # pixel width
                  'height':1280,                  # pixel height
                  'dpi':None,                     # DPI - Use to estimate real-world width / height if needed?
                  'bytes':None,                   # bytes size of this version
                  'content_type':ims['mime'],     # Image mime-type
                  'uri_external':None,            # External URI.
                  }
                 ]
        
        xid = make_id(jj_top['_id'])

        original_id = jj_top['_id']#.split('_')[-1]

        #cache_url = make_cache_url(jj_top['_id'])
        #if not cache_url:
        #    continue

        attribution = []
        artist_names = None
        
        if jj['author_name']:
            artist_names = [jj['author_name']]
            attribution = [{'name':jj['author_name'],
                            'role':'artist',
                            'details':None,
                            }]

        source_tags = ['pexels.com']
        
        if jj['source_name']:
            source_tags.append(jj['source_name'])
        
        source_tags = list(set([(x[len('https://'):] if x.startswith('https://') else x) for x in source_tags]))
        source_tags = list(set([(x[len('http://'):] if x.startswith('http://') else x) for x in source_tags]))
        source_tags = list(set([(x[len('www.'):] if x.startswith('www.') else x) for x in source_tags]))
        
        hh = {'_id':xid,
              'aspect_ratio':ims['aspect_ratio'],
              'native_source_id':jj_top['_id'],
              'native_id':original_id,
              'source_dataset':'pexels',
              'source':{'name':jj['source_name'] or 'pexels',
                        #'url':jj['source_url'] or 'https://www.pexels.com/',
                        'url':jj['the_canon'],
                        },
              'source_tags':source_tags,                       # For easy Indexer faceting
              'license_tags':['CC0'],                          # For easy Indexer faceting
              'img_data':jj_top['img_data'],                   # Data URI of this version -- only for thumbnails.
              'artist_names':artist_names,
              'attribution':attribution,                               ## Artist / Entity names.
              'url_shown_at':{'url':jj['the_canon']},
              'url_direct':None,
              #'url_direct_cache':{'url':cache_url},
              'date_source_version':None,                     # Date this snapshot started
              'date_captured':None,
              'date_created_original':None,                   # Actual creation date.
              'date_created_at_source':None,                  # Item created at data source.
              'title':[' '.join(jj['the_kw'])],               # Title string(s)
              'keywords':jj['the_kw'],                        # Keywords
              'orientation':None,                             # Should photo be rotated?
              #'editorial_source_name':None,                   #
              #'editorial_source':None,
              'camera_exif':{},                               # Camera Exif data
              'license_name':"CC0",
              'license_name_long':"Creative Commons Zero (CC0)",
              'license_url':None,
              'licenses':[                                    ## List of licenses:
                  ## TODO - Correct but hard-coded:
                  {'name':"CC0",                               # License name
                   'name_long':"Creative Commons Zero (CC0)",  # Long name
                   'attribute_to':[],                          # Attribute to this person / organization / paper citation.
                   'details':[],                               # Additional license details text
                   }],
              'sizes':sizes,
              'location':{
                  'lat_lon':None,        ## Latitude / Longitude.
                  'place_name':[],       # Place Name
                  },
              'derived_qualities':{      ## Possibly derived from image / from other metadata:
                  'general_type':None,   # (photo, illustration, GIF, face)
                  'colors':None,         # Dominant colors.
                  'has_people':None,     # Has people?
                  'time_period':None,    # (contemporary, 1960s, etc)
                  'medium':None,         # (photograph, drawing, vector, etc)
                  'predicted_tags':None, #
                  },
              'transient_info':{         ## Information that may frequently change.
                  'score_hotness'        # Popularity / newness
                  'views':None,          #
                  'likes':None,          #
                  },
              }

        yield hh



def normalize_dpla(iter_json):
    """
    // SCHEMA: dpla:

    {
        "_id": "dpla_http://dp.la/api/items/ac25f47b73a06cd9035737bfff02d997",
        "source_record": {
            "_score": 0,
            "_type": "item",
            "_id": "digitalnc--urn:brevard.lib.unc.eduunc_ncmaps:oai:dc.lib.unc.edu:ncmaps/6766",
            "_index": "dpla-20150410-144958",
            "_source": {
                "@context": "http://dp.la/api/items/context",
                "dataProvider": "University of North Carolina at Chapel Hill",
                "admin": {
                    "sourceResource": {
                        "title": "[Highway maintenance map of] Gaston County, North Carolina"
                    },
                    "validation_message": null,
                    "valid_after_enrich": true
                },
                "@id": "http://dp.la/api/items/ac25f47b73a06cd9035737bfff02d997",
                "_rev": "2-5d94f1105d58eb8a30b2a7fdef2f9e02",
                "object": "http://dc.lib.unc.edu/utils/getthumbnail/collection/ncmaps/id/6766",
                "aggregatedCHO": "#sourceResource",
                "provider": {
                    "@id": "http://dp.la/api/contributor/digitalnc",
                    "name": "North Carolina Digital Heritage Center"
                },
                "ingestDate": "2016-04-11T02:11:17.720639Z",
                "@type": "ore:Aggregation",
                "ingestionSequence": 22,
                "isShownAt": "http://dc.lib.unc.edu/u?/ncmaps,6766",
                "sourceResource": {
                    "publisher": [
                        "North Carolina Department of Cultural Resources"
                    ],
                    "description": [
                        "This map was prepared from data collected by the state-wide highway planning survey as one of a series of county maps showing the federal, state, and county highway systems in each county as of January 1, 1962. The map was published as part of a paper bound set of maps entitled \"North Carolina State Highway Commission Municipal, State, Primary and Interstate Highway Systems Maintenance Maps by Counties with Enlarged Municipal and Suburban Areas.\" Schools, churches, and other landmarks are noted; and rivers, creeks, and other topographical features are identified. A legend is in the right margin. A location map and a key to county road numbers appear as insets. Maps of Cherryville, High Shoals, Hardins, Mountain View, Duke Power Village, Dallas, Stanley, Bessemer City, and Belmont-Gastonia-Lowell-McAdenville-Mount Holly and vicinity are found in the margins of sheet one and on sheet two.",
                        "This map was formerly a part of a set of maps classified as M.C. 7.5."
                    ],
                    "language": [
                        {
                            "iso639_3": "eng",
                            "name": "English"
                        }
                    ],
                    "format": "Maps",
                    "type": "image",
                    "rights": "This item is presented courtesy of the State Archives of North Carolina, for research and educational purposes. Prior permission from the State Archives is required for any commercial use.",
                    "collection": {
                        "description": "",
                        "@id": "http://dp.la/api/collections/047f1ab40471621d8903c6002272548e",
                        "id": "047f1ab40471621d8903c6002272548e",
                        "title": "North Carolina Maps"
                    },
                    "stateLocatedIn": [
                        {
                            "name": "North Carolina"
                        }
                    ],
                    "creator": [
                        "North Carolina State Highway Commission.",
                        "United States. Bureau of Public Roads."
                    ],
                    "spatial": [
                        {
                            "county": "Gaston County",
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "Gaston County (N.C.)",
                            "coordinates": "35.2944107056, -81.180229187"
                        },
                        {
                            "county": "Gaston County",
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "Belmont (N.C.)",
                            "coordinates": "35.2442016602, -81.0380172729"
                        },
                        {
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "Bessemer City (N.C.)",
                            "coordinates": "35.2849502563, -81.2849884033"
                        },
                        {
                            "county": "Lancaster County",
                            "country": "United States",
                            "state": "South Carolina",
                            "name": "Catawba River (N.C.)",
                            "coordinates": "34.7926292419, -80.8799362183"
                        },
                        {
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "Cherryville (N.C.)",
                            "coordinates": "35.382068634, -81.3797683716"
                        },
                        {
                            "county": "Gaston County",
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "Dallas (N.C.)",
                            "coordinates": "35.3160095215, -81.1758728027"
                        },
                        {
                            "name": "Duke Power Village (Gaston County, N.C.)"
                        },
                        {
                            "name": "Gastonia (N.C.)"
                        },
                        {
                            "county": "Gaston County",
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "Hardins (N.C.)",
                            "coordinates": "35.3792381287, -81.1915283203"
                        },
                        {
                            "county": "Gaston County",
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "High Shoals (N.C.)",
                            "coordinates": "35.4015312195, -81.2019195557"
                        },
                        {
                            "county": "Gaston County",
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "Lowell (N.C.)",
                            "coordinates": "35.266998291, -81.1015777588"
                        },
                        {
                            "county": "Gaston County",
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "McAdenville (N.C.)",
                            "coordinates": "35.2587890625, -81.0773468018"
                        },
                        {
                            "county": "Gaston County",
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "Mount Holly (N.C.)",
                            "coordinates": "35.2966194153, -81.016242981"
                        },
                        {
                            "name": "Mountain View (Gaston County, N.C.)"
                        },
                        {
                            "county": "Gaston County",
                            "country": "United States",
                            "state": "North Carolina",
                            "name": "Stanley (N.C.)",
                            "coordinates": "35.359161377, -81.0956726074"
                        },
                        {
                            "county": "Apache County",
                            "country": "United States",
                            "state": "Arizona",
                            "name": "-81.43",
                            "coordinates": "34.2955093384, -109.81829071"
                        },
                        {
                            "name": "-80.937222"
                        },
                        {
                            "name": "35.390833"
                        },
                        {
                            "name": "35.141389"
                        }
                    ],
                    "date": {
                        "begin": "1962",
                        "end": "1962",
                        "displayDate": "1962-1962"
                    },
                    "title": [
                        "[Highway maintenance map of] Gaston County, North Carolina"
                    ],
                    "identifier": [
                        "MC.040.1962n",
                        "MARS Id 3.1.37.20",
                        "http://dc.lib.unc.edu/u?/ncmaps,6766"
                    ],
                    "@id": "http://dp.la/api/items/ac25f47b73a06cd9035737bfff02d997#sourceResource",
                    "subject": [
                        {
                            "name": "Gaston County (N.C.)--Maps"
                        },
                        {
                            "name": "Roads--North Carolina--Gaston County--Maps"
                        }
                    ]
                },
                "ingestType": "item",
                "_id": "digitalnc--urn:brevard.lib.unc.eduunc_ncmaps:oai:dc.lib.unc.edu:ncmaps/6766",
                "originalRecord": {
                    "about": {
                        "oaiProvenance:provenance": {
                            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                            "xsi:schemaLocation": "http://www.openarchives.org/OAI/2.0/provenance http://www.openarchives.org/OAI/2.0/provenance.xsd",
                            "oaiProvenance:originDescription": {
                                "harvestDate": "2015-12-09",
                                "oaiProvenance:metadataNamespace": "http://www.openarchives.org/OAI/2.0/",
                                "oaiProvenance:baseURL": "http://dc.lib.unc.edu/cgi-bin/oai.exe",
                                "oaiProvenance:datestamp": "2015-12-09",
                                "altered": "true",
                                "oaiProvenance:identifier": "oai:dc.lib.unc.edu:ncmaps/6766"
                            },
                            "xmlns:oaiProvenance": "http://www.openarchives.org/OAI/2.0/provenance"
                        }
                    },
                    "collection": {
                        "description": "",
                        "@id": "http://dp.la/api/collections/047f1ab40471621d8903c6002272548e",
                        "id": "047f1ab40471621d8903c6002272548e",
                        "title": "North Carolina Maps"
                    },
                    "header": {
                        "datestamp": "2015-12-09",
                        "identifier": "urn:brevard.lib.unc.eduunc_ncmaps:oai:dc.lib.unc.edu:ncmaps/6766",
                        "setSpec": "unc_ncmaps"
                    },
                    "provider": {
                        "@id": "http://dp.la/api/contributor/digitalnc",
                        "name": "North Carolina Digital Heritage Center"
                    },
                    "id": "urn:brevard.lib.unc.eduunc_ncmaps:oai:dc.lib.unc.edu:ncmaps/6766",
                    "metadata": {
                        "mods": {
                            "physicalDescription": {
                                "form": "Maps"
                            },
                            "xmlns": "http://www.loc.gov/mods/v3",
                            "name": [
                                {
                                    "namePart": "North Carolina State Highway Commission.",
                                    "role": {
                                        "roleTerm": "creator"
                                    }
                                },
                                {
                                    "namePart": "United States. Bureau of Public Roads.",
                                    "role": {
                                        "roleTerm": "creator"
                                    }
                                }
                            ],
                            "language": {
                                "languageTerm": "English"
                            },
                            "titleInfo": {
                                "title": "[Highway maintenance map of] Gaston County, North Carolina"
                            },
                            "subject": [
                                {
                                    "geographic": "Gaston County (N.C.)"
                                },
                                {
                                    "geographic": "Belmont (N.C.)"
                                },
                                {
                                    "geographic": "Bessemer City (N.C.)"
                                },
                                {
                                    "geographic": "Catawba River (N.C.)"
                                },
                                {
                                    "geographic": "Cherryville (N.C.)"
                                },
                                {
                                    "geographic": "Dallas (N.C.)"
                                },
                                {
                                    "geographic": "Duke Power Village (Gaston County, N.C.)"
                                },
                                {
                                    "geographic": "Gastonia (N.C.)"
                                },
                                {
                                    "geographic": "Hardins (N.C.)"
                                },
                                {
                                    "geographic": "High Shoals (N.C.)"
                                },
                                {
                                    "geographic": "Lowell (N.C.)"
                                },
                                {
                                    "geographic": "McAdenville (N.C.)"
                                },
                                {
                                    "geographic": "Mount Holly (N.C.)"
                                },
                                {
                                    "geographic": "Mountain View (Gaston County, N.C.)"
                                },
                                {
                                    "geographic": "Stanley (N.C.)"
                                },
                                {
                                    "geographic": "-81.43"
                                },
                                {
                                    "geographic": "-80.937222"
                                },
                                {
                                    "geographic": "35.390833"
                                },
                                {
                                    "geographic": "35.141389"
                                },
                                {
                                    "topic": "Gaston County (N.C.)--Maps."
                                },
                                {
                                    "topic": "Roads--North Carolina--Gaston County--Maps."
                                }
                            ],
                            "note": [
                                {
                                    "#text": "This map was prepared from data collected by the state-wide highway planning survey as one of a series of county maps showing the federal, state, and county highway systems in each county as of January 1, 1962. The map was published as part of a paper bound set of maps entitled \"North Carolina State Highway Commission Municipal, State, Primary and Interstate Highway Systems Maintenance Maps by Counties with Enlarged Municipal and Suburban Areas.\" Schools, churches, and other landmarks are noted; and rivers, creeks, and other topographical features are identified. A legend is in the right margin. A location map and a key to county road numbers appear as insets. Maps of Cherryville, High Shoals, Hardins, Mountain View, Duke Power Village, Dallas, Stanley, Bessemer City, and Belmont-Gastonia-Lowell-McAdenville-Mount Holly and vicinity are found in the margins of sheet one and on sheet two.",
                                    "type": "content"
                                },
                                {
                                    "#text": "This map was formerly a part of a set of maps classified as M.C. 7.5.",
                                    "type": "content"
                                },
                                {
                                    "#text": "University of North Carolina at Chapel Hill",
                                    "type": "ownership"
                                }
                            ],
                            "accessCondition": "This item is presented courtesy of the State Archives of North Carolina, for research and educational purposes. Prior permission from the State Archives is required for any commercial use.",
                            "version": "3.4",
                            "originInfo": {
                                "publisher": "North Carolina Department of Cultural Resources",
                                "dateCreated": [
                                    {
                                        "#text": "1962",
                                        "keyDate": "yes"
                                    },
                                    {
                                        "#text": "1962",
                                        "keyDate": "yes"
                                    }
                                ]
                            },
                            "location": [
                                {
                                    "url": {
                                        "usage": "primary display",
                                        "access": "object in context",
                                        "#text": "http://dc.lib.unc.edu/u?/ncmaps,6766"
                                    }
                                },
                                {
                                    "url": {
                                        "access": "preview",
                                        "#text": "http://dc.lib.unc.edu/utils/getthumbnail/collection/ncmaps/id/6766"
                                    }
                                }
                            ],
                            "xmlns:oai_dc": "http://www.openarchives.org/OAI/2.0/oai_dc/",
                            "genre": "Image",
                            "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                            "identifier": [
                                "MC.040.1962n",
                                "MARS Id 3.1.37.20",
                                "http://dc.lib.unc.edu/u?/ncmaps,6766"
                            ],
                            "xsi:schemaLocation": "http://www.loc.gov/mods/v3 http://www.loc.gov/standards/mods/v3/mods-3-4.xsd",
                            "xmlns:dc": "http://purl.org/dc/elements/1.1/"
                        }
                    }
                },
                "id": "ac25f47b73a06cd9035737bfff02d997"
            }
        },
        "description": "This map was prepared from data collected by the state-wide highway planning survey as one of a series of county maps showing the federal, state, and county highway systems in each county as of January 1, 1962. The map was published as part of a paper bound set of maps entitled \"North Carolina State Highway Commission Municipal, State, Primary and Interstate Highway Systems Maintenance Maps by Counties with Enlarged Municipal and Suburban Areas.\" Schools, churches, and other landmarks are noted; and rivers, creeks, and other topographical features are identified. A legend is in the right margin. A location map and a key to county road numbers appear as insets. Maps of Cherryville, High Shoals, Hardins, Mountain View, Duke Power Village, Dallas, Stanley, Bessemer City, and Belmont-Gastonia-Lowell-McAdenville-Mount Holly and vicinity are found in the margins of sheet one and on sheet two.; This map was formerly a part of a set of maps classified as M.C. 7.5.",
        "artist": "North Carolina State Highway Commission.; United States. Bureau of Public Roads.",
        "shown_at_url": {'url':"http://dc.lib.unc.edu/u?/ncmaps,6766"},
        "dataset": "dpla",
        "licenses": [
            {
                "raw": "This item is presented courtesy of the State Archives of North Carolina, for research and educational purposes. Prior permission from the State Archives is required for any commercial use."
            }
        ],
        "date": "2016-04-11T02:11:17.720639+00:00",
        "title": "[Highway maintenance map of] Gaston County, North Carolina",
        "direct_url": "http://dc.lib.unc.edu/utils/getthumbnail/collection/ncmaps/id/6766"
    }
    """

    #('DPLA_COMMON_ALL', set([u'sourceResource', u'object', u'aggregatedCHO', u'ingestDate', u'originalRecord', u'ingestionSequence', u'isShownAt', u'provider', u'@context', u'ingestType', u'_id', u'@id', u'id']))
    #('DPLA_COMMON_SOME', set([u'hasView', u'admin', u'_rev', u'intermediateProvider', u'dataProvider', u'@type']))

    assert not hasattr(iter_json, 'next'), 'Should be function that returns iterator when called, to allow restarting.'

    for jj_top in iter_json():

        jj = jj_top['source_record'] ## Ignore the minimal normalization we did initially. Look at source.
        jj = jj['_source']           ## Ignore everything except for the "sourceResource" info.

        st = get_image_stats(jj_top['img_data'])

        sizes = [{'width':st['width'],            # pixel width - TODO
                  'height':st['height'],          # pixel height - TODO
                  'dpi':None,                     # DPI - Use to estimate real-world width / height if needed?
                  'bytes':None,                   # bytes size of this version
                  'content_type':st['mime'],      # Image mime-type
                  'uri_external':jj['object'],    # External URI.
                  }
                 ]

        artists = None
        artist_names = []
        try:
            artists = [{'name':x} for x in jj['sourceResource']['creator'] if len(x) > 1]
            artist_names.append([x['name'] for x in artists if len(x['name']) > 1]) ## TODO, why so many single-letter names?
        except:
            pass

        xid = make_id(jj_top['_id'])

        source_tags = ['dp.la']
        
        try:
            prov = jj_top['source_record']['_source']['dataProvider']
        except:
            prov = jj_top['source_record']['_source']['provider']
            
        if (type(prov) == list):
            source_tags.extend(prov)

        elif (type(prov) == dict) and ('name' in prov):
            source_tags.extend(prov['name'])
        
        source_tags = list(set([(x[len('https://'):] if x.startswith('https://') else x) for x in source_tags]))
        source_tags = list(set([(x[len('http://'):] if x.startswith('http://') else x) for x in source_tags]))
        source_tags = list(set([(x[len('www.'):] if x.startswith('www.') else x) for x in source_tags]))
        
        the_title = get_shallowest_matching(jj, 'title')
        if isinstance(the_title, basestring):
            the_title = [the_title]

        try:
            desc = get_shallowest_matching(jj, 'description')
            if type(desc) == list:
                desc = ' '.join(desc)
        except:
            print ('EXCEPT_DESC',desc)
            desc = None
        
        hh = {'_id':xid,
              'aspect_ratio':st['aspect_ratio'],
              'native_id':jj_top['_id'],
              'source_dataset':'dpla',
              'source':{'name':'dpla',
                        'url':'https://dp.la/',
                        },
              'source_tags':source_tags,      # For easy indexer faceting
              'img_data':jj_top['img_data'],  # Data URI of this version -- only for thumbnails.
              'artist_names':artist_names,
              'providers_list':[                              ## List of providers. Top most recent.
                  {'name':'dpla'},
                  {'name':jj['provider']['name']},
                  ],
              'url_shown_at':{'url':jj['isShownAt']},
              'url_direct':{'url':jj['object']},
              #'url_direct_cache':{'url':make_cache_url(jj_top['_id'])},
              'date_source_version':None,                     # Date this snapshot started
              'date_captured':None,
              'date_created_original':get_shallowest_matching(jj, 'displayDate'), # Actual creation date.
              'date_created_at_source':None,                  # Item created at data source.
              'title':the_title,    # Title string(s)
              'description':desc, # Description
              'attribution':artists,                          ## Artist / Entity names.
              'keywords':[],                                  # Keywords
              'orientation':None,                             # Should photo be rotated?
              #'editorial_source_name':None, #
              #'editorial_source':None,
              'camera_exif':{},                                # Camera Exif data
              'licenses':[                                    ## List of licenses:
                  {'details':get_shallowest_matching(jj, "rights"),  # Additional license details text
                   }],
              'sizes':sizes,
              'location':{
                  'lat_lon':None,        ## Latitude / Longitude.
                  'place_name':[],       # Place Name
                  },
              'derived_qualities':{      ## Possibly derived from image / from other metadata:
                  'general_type':None,#get_shallowest_matching(jj, 'format'),   # (photo, illustration, GIF, face)
                  'colors':None,         # Dominant colors.
                  'has_people':None,     # Has people?
                  'time_period':None,    # (contemporary, 1960s, etc)
                  'medium':None,         # (photograph, drawing, vector, etc)
                  'predicted_tags':None, #
                  },
              'transient_info':{         ## Information that may frequently change.
                  'score_hotness'        # Popularity / newness
                  'views':None,          #
                  'likes':None,          #
                  },
              }

        yield hh



def normalize_mirflickr1mm(iter_json):
    """
    // SCHEMA: mirflickr1mm:

    {
        "source_record": {
            "exif": {
                "-Date and Time": "2008:12:05 12:50:54",
                "-Image Width": "3072",
                "-Picture Info": "9, 0, 3072, 2304, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 32, 5",
                "-Focal Plane Resolution Unit": "2",
                "-X-Resolution": "180/1",
                "-Resolution Unit": "2",
                "-Tag::Canon::0x001D": "32, 1, 0, 2, 2, 2, 2, 0, 0, 0, 0, 39, 0, 0, 0, 0",
                "-Compression": "6",
                "-Pixel X-Dimension": "3072",
                "-Y-Resolution": "180/1",
                "-Date and Time (Digitized)": "2008:12:05 12:50:54",
                "-Model": "Canon PowerShot A620",
                "-Focal Plane X-Resolution": "3072000/284",
                "-Interop Footer": "1",
                "-Orientation": "1",
                "-Camera Settings": "92, 2, 0, 3, 5, 0, 0, 4, 65535, 1, 0, 0, 0, 0, 0, 0, 15, 3, 1, 16385, 0, 32767, 65535, 29200, 7300, 1000, 130, 221, 65535, 8200, 0, 0, 0, 0, 65535, 41, 3072, 3072, 0, 0, 0, 0, 32767, 32767, 0, 0",
                "-Aperture": "130/32",
                "-Maximum Lens Aperture": "130/32",
                "-Flash Info": "0, 0, 0, 0",
                "-Image Type": "IMG:PowerShot A620 JPEG",
                "-Image Height": "2304",
                "-Flash": "89",
                "-Tag::Canon::0x001E": "16778752",
                "-Sensing Method": "2",
                "-Focal Length": "2, 29200, 291, 218",
                "-Digital Zoom Ratio": "3072/3072",
                "-Exposure": "1/60",
                "-Make": "Canon",
                "-Focal Plane Y-Resolution": "2304000/213",
                "-Tag::Canon::0x0013": "0, 0, 0, 0",
                "-Metering Mode": "5",
                "-Tag::Canon::0x0018": "0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0",
                "-YCbCr Positioning": "1",
                "-Camera Info": "1, 2, 0, 398, 31, 0, 31, 10, 0, 0, 45, 0, 382, 398, 400, 0, 4294967287, 382, 553, 59, 61, 372, 188, 0, 188, 144, 0, 0, 111, 242, 353, 0, 0, 0, 0, 0, 0, 631, 0, 242, 353, 4294966985, 456, 1214, 1517, 4, 398, 52, 1148, 1882, 1888, 1148, 1, 576, 400, 413, 555, 8, 4294967290, 0, 511, 0, 0, 0, 0, 1688, 5, 0, 0, 0, 0, 2, 0, 1804, 2025, 0, 0, 511, 0, 4294943088, 4, 9, 1625, 1629, 1630, 1625, 1626, 1631, 1624, 1626, 1628, 17, 7, 1431876402",
                "-Compressed Bits per Pixel": "3/1",
                "-Model ID": "24641536",
                "-Exposure Bias": "0/3",
                "-Date and Time (Original)": "2008:12:05 12:50:54",
                "-Image Number": "1050522",
                "-Pixel Y-Dimension": "2304",
                "-Color Space": "1",
                "-Firmware Version": "Firmware Version 1.00",
                "-Shot Info": "68, 63, 128, 137, 130, 189, 0, 0, 0, 0, 8, 0, 0, 272, 0, 0, 0, 0, 1, 1608, 0, 133, 192, 0, 0, 3, 250, 0, 0, 0, 0, 0, 0, 800",
                "-Shutter Speed": "189/32"
            },
            "license": {
                "Photo id": "3084779859",
                "Owner name": "G\u0102\u0160rard d'Alboy",
                "Owner username": "G\u0102\u0160rard d'Alboy",
                "Photo url": "http://farm4.static.flickr.com/3262/3084779859_b0eabd0682.jpg",
                "Date uploaded": "2008-12-5",
                "Owner id": "15558803@N06",
                "Picture title": "DORMELLES - Fort de Challeau",
                "Web url": "http://www.flickr.com/photos/15558803@N06/3084779859/"
            },
            "tags": [
                "DORMELLES",
                "77",
                "GERARD d'ALBOY",
                "VIEWS 1881"
            ]
        },
        "artist": "G\u0102\u0160rard d'Alboy",
        "title": "DORMELLES - Fort de Challeau",
        "dataset": "mirflickr1mm",
        "licenses": [],
        "keywords": "DORMELLES, 77, GERARD d'ALBOY, VIEWS 1881",
        "date_created": "2008-12-05T00:00:00",
        "_id": "mirflickr1mm_333909"
    }

    """

    #('DPLA_COMMON_ALL', set([u'sourceResource', u'object', u'aggregatedCHO', u'ingestDate', u'originalRecord', u'ingestionSequence', u'isShownAt', u'provider', u'@context', u'ingestType', u'_id', u'@id', u'id']))
    #('DPLA_COMMON_SOME', set([u'hasView', u'admin', u'_rev', u'intermediateProvider', u'dataProvider', u'@type']))

    assert not hasattr(iter_json, 'next'), 'Should be function that returns iterator when called, to allow restarting.'

    for jj_top in iter_json():

        jj = jj_top['source_record'] ## Ignore the minimal normalization we did initially. Look at source.

        st = get_image_stats(jj_top['img_data'])

        licenses = []
        if jj['license'].get('License'):
            licenses = [{'name_long':jj['license'].get('License')}]

        sizes = [{'width':st['width'],            # pixel width
                  'height':st['height'],          # pixel height
                  'dpi':None,                     # DPI - Use to estimate real-world width / height if needed?
                  'bytes':None,                   # bytes size of this version
                  'content_type':st['mime'],      # Image mime-type
                  'uri_external':jj['license'].get('Web url'),    # External URI.
                  }
                 ]

        artist_names = []
        attribution = []
        if jj['license'].get("Owner name"):
            artist_names.append(jj['license'].get("Owner name"))
            attribution.append({'name':jj['license'].get("Owner name"),
                                'role':'artist',
                                })
        
        xid = make_id(jj_top['_id'])
        
        hh = {'_id':xid,
              'aspect_ratio':st['aspect_ratio'],
              'native_id':jj_top['_id'],
              'source_dataset':'mirflickr1mm',
              'source':{'name':'mirflickr1mm',
                        'url':None,
                        },
              'img_data':jj_top['img_data'],                  # Data URI of this version -- only for thumbnails.
              'artist_names':artist_names,
              'providers_list':[                              ## List of providers, most recent first.
                  {'name':'flickr'},
              ],
              'url_shown_at':{'url':jj['license'].get('Photo url')},
              'url_direct':{'url':jj['license'].get('Web url')},
              #'url_direct_cache':{'url':make_cache_url(jj_top['_id'])},
              'date_source_version':None,                     # Date this snapshot started
              'date_captured':get_shallowest_matching(jj, "-Date and Time"),
              'date_created_original':get_shallowest_matching(jj, "-Date and Time"),  # Actual creation date.
              'date_created_at_source':jj['license']['Date uploaded'],      # Item created at data source.
              'title':[jj['license'].get("Picture title")],    # Title string(s)
              'description':None,                        # Description
              'attribution':attribution,                       ## Artist / Entity names.
              'keywords':[],                                  # Keywords
              'orientation':None,                             # Should photo be rotated?
              #'editorial_source_name':None, #
              #'editorial_source':None,
              'camera_exif':jj['exif'],                       # Camera Exif data
              'licenses':licenses,                            ## List of licenses:
              'sizes':sizes,
              'location':{},
              'derived_qualities':{      ## Possibly derived from image / from other metadata:
                  'general_type':None,   # (photo, illustration, GIF, face)
                  'colors':None,         # Dominant colors.
                  'has_people':None,     # Has people?
                  'time_period':None,    # (contemporary, 1960s, etc)
                  'medium':None,         # (photograph, drawing, vector, etc)
                  'predicted_tags':None, #
                  },
              'transient_info':{         ## Information that may frequently change.
                  'score_hotness'        # Popularity / newness
                  'views':None,          #
                  'likes':None,          #
                  },
              }

        yield hh



def normalize_places(iter_json):
    """
    // SCHEMA: places:
    {
        "licenses": [
            {
                "attribution": "B. Zhou, A. Lapedriza, J. Xiao, A. Torralba, and A. Oliva. Learning Deep Features for Scene Recognition using Places Database. Advances in Neural Information Processing Systems 27 (NIPS), 2014..",
                "name": "Creative Common License (Attribution CC BY)"
            }
        ],
        "_id": "places_0c0273458459d39dfe17369d0622e643",
        "source_record": {
            "fn": "data/vision/torralba/deeplearning/images256/a/art_studio/gsun_0c0273458459d39dfe17369d0622e643.jpg"
        },
        "title": "Art Studio"
    }
    """

    assert not hasattr(iter_json, 'next'), 'Should be function that returns iterator when called, to allow restarting.'

    for jj_top in iter_json():

        jj = jj_top['source_record'] ## Ignore the minimal normalization we did initially. Look at source.

        st = get_image_stats(jj_top['img_data'])

        sizes = [{'content_type':st['mime'],      # Image mime-type
                  }
                 ]

        artists = []

        xid = make_id(jj_top['_id'])
        
        hh = {'_id':xid,
              'aspect_ratio':st['aspect_ratio'],
              'native_id':jj_top['_id'],
              'source_dataset':'places205',
              'source':{'name':'places205',
                        'url':None,
                        },
              'img_data':jj_top['img_data'],             # Data URI of this version -- only for thumbnails.
              'artist_names':None,
              'providers_list':[{'name':'places205'}],   ## List of providers, most recent first.
              'url_shown_at':None,
              'url_direct':None,
              #'url_direct_cache':{'url':make_cache_url(jj_top['_id'])},
              'date_source_version':None,                # Date this snapshot started
              'date_captured':None,
              'date_created_original':None,              # Actual creation date.
              'date_created_at_source':None,             # Item created at data source.
              'title':[jj_top["title"]],                 # Title string(s)
              'description':None,                        # Description
              'attribution':[],                          ## Artist / Entity names.
              'keywords':[],                                  # Keywords
              'orientation':None,                             # Should photo be rotated?
              #'editorial_source_name':None, #
              #'editorial_source':None,
              'camera_exif':None,                             # Camera Exif data
              'licenses':[],                                  ## List of licenses:
              'sizes':sizes,
              'location':{},
              'derived_qualities':{      ## Possibly derived from image / from other metadata:
                  'general_type':None,   # (photo, illustration, GIF, face)
                  'colors':None,         # Dominant colors.
                  'has_people':None,     # Has people?
                  'time_period':None,    # (contemporary, 1960s, etc)
                  'medium':None,         # (photograph, drawing, vector, etc)
                  'predicted_tags':None, #
                  },
              'transient_info':{         ## Information that may frequently change.
                  'score_hotness'        # Popularity / newness
                  'views':None,          #
                  'likes':None,          #
                  },
              }

        yield hh


def simple_schema_validate(record,
                           ):
    """
    1) Apply the normalizer
    2) Apply ES transforms.
    3) Apply post-ingest transforms.
    4) Check for all the following fields, expected by frontend:
    
    === FROM FRONTEND:
    
    function normalizeImageJson(json) {
      const {
        artist_name,
        keywords,
        license,
        id,
        title,
        source,
        sizes,
        image_url
      } = json

      const author = artist_name && toTitleCase(artist_name)

      const size = sizes[0];
      const { width, height } = size;

      return {
        id,
        attribution: author,
        title,
        imageUrl: image_url,
        keywords,
        license: license.name,
        source,
        width,
        height
      }
    }
    """
    import copy
    
    record = copy.deepcopy(record)

    ## Apply ES transform:

        
    r2 = {'_source':{}}
    for k, v in record.items():
        if k.startswith('_'):
            r2[k] = v
        else:
            r2['_source'][k] = v
    record = r2
    
    ## Apply post-ingest transforms:

    record['_source']['url_direct_cache'] = {'url':None}

    apply_post_ingestion_normalizers([record], schema_variant = 'new')

    #print 'keys',record.keys()

    #'id', 'image_url'
    
    missing = set(['artist_name', 'keywords', 'license',
                    'title', 'source', 'sizes',]).difference(record['_source'].keys())
    
    assert not missing, ('MISSING -', missing)
    
    assert record['_source']['license'] is not None, ('MISSING - license',)
    
    assert 'name' in record['_source']['license'], ('MISSING - license.name',record['_source']['license'])
    
    print 'PASSED simple_schema_validate()'
        
    
    
        
def normalize_500px(iter_json):
    """
    {
		"id": 165862179,
		"user_id": 8187387,
		"name": "The light",
		"description": null,
		"camera": "FinePix HS20EXR",
		"lens": null,
		"focal_length": "22.1",
		"iso": "400",
		"shutter_speed": "1/45",
		"aperture": "4.5",
		"times_viewed": 862,
		"rating": 78.4,
		"status": 1,
		"created_at": "2016-08-01T12:06:09-04:00",
		"category": 0,
		"location": null,
		"latitude": 47.1718531,
		"longitude": 19.5013194000001,
		"taken_at": "2014-10-18T08:04:01-04:00",
		"hi_res_uploaded": 0,
		"for_sale": false,
		"width": 3264,
		"height": 2448,
		"votes_count": 44,
		"favorites_count": 0,
		"comments_count": 0,
		"nsfw": false,
		"sales_count": 0,
		"for_sale_date": null,
		"highest_rating": 92.0,
		"highest_rating_date": "2016-08-02T00:02:10-04:00",
		"license_type": 4,
		"converted": 27,
		"collections_count": 0,
		"crop_version": 3,
		"privacy": false,
		"profile": true,
		"image_url": ["https://drscdn.500px.org/photo/165862179/w%3D70_h%3D70/e4601655f405bc0c354ff1ad306d996e?v=3", "https://drscdn.500px.org/photo/165862179/q%3D50_w%3D140_h%3D140/50519f2a27543fc98277068f7f1dce5a?v=3", "https://drscdn.500px.org/photo/165862179/q%3D80_h%3D300/bcaf3ff5e8f7fd0fb22bc18e211ff1cf", "https://drscdn.500px.org/photo/165862179/q%3D80_h%3D450/7f086e6acc50b53cb24944e88987c034", "https://drscdn.500px.org/photo/165862179/q%3D80_h%3D600_k%3D1/cfd9d62daffb7fc5de30e190a5c1ef4a", "https://drscdn.500px.org/photo/165862179/q%3D80_m%3D1000_k%3D1/6515263ce3e2fbd42a288e28279f9818", "https://drscdn.500px.org/photo/165862179/q%3D80_m%3D1500_k%3D1/d48e6ec5ccaff39e34495b957cadb878", "https://drscdn.500px.org/photo/165862179/q%3D80_m%3D2000_k%3D1/9ce813eb3eb1b3e4717f5b695eb9f953", "https://drscdn.500px.org/photo/165862179/m%3D2048_k%3D1/ae57f79d32bcb15d113b5880d2ae216a", "https://drscdn.500px.org/photo/165862179/m%3D900/6053571d0eaffae1c1a764f3943c21a9", "https://drscdn.500px.org/photo/165862179/m%3D900_s%3D1_k%3D1_a%3D1/bbfd96899c225596bf6b91235ecd0356?v=3"],
		"images": [{
			"size": 1,
			"url": "https://drscdn.500px.org/photo/165862179/w%3D70_h%3D70/e4601655f405bc0c354ff1ad306d996e?v=3",
			"https_url": "https://drscdn.500px.org/photo/165862179/w%3D70_h%3D70/e4601655f405bc0c354ff1ad306d996e?v=3",
			"format": "jpeg"
		}, {
			"size": 2,
			"url": "https://drscdn.500px.org/photo/165862179/q%3D50_w%3D140_h%3D140/50519f2a27543fc98277068f7f1dce5a?v=3",
			"https_url": "https://drscdn.500px.org/photo/165862179/q%3D50_w%3D140_h%3D140/50519f2a27543fc98277068f7f1dce5a?v=3",
			"format": "jpeg"
		}, {
			"size": 4,
			"url": "https://drscdn.500px.org/photo/165862179/m%3D900/6053571d0eaffae1c1a764f3943c21a9",
			"https_url": "https://drscdn.500px.org/photo/165862179/m%3D900/6053571d0eaffae1c1a764f3943c21a9",
			"format": "jpeg"
		}, {
			"size": 14,
			"url": "https://drscdn.500px.org/photo/165862179/m%3D900_s%3D1_k%3D1_a%3D1/bbfd96899c225596bf6b91235ecd0356?v=3",
			"https_url": "https://drscdn.500px.org/photo/165862179/m%3D900_s%3D1_k%3D1_a%3D1/bbfd96899c225596bf6b91235ecd0356?v=3",
			"format": "jpeg"
		}, {
			"size": 31,
			"url": "https://drscdn.500px.org/photo/165862179/q%3D80_h%3D450/7f086e6acc50b53cb24944e88987c034",
			"https_url": "https://drscdn.500px.org/photo/165862179/q%3D80_h%3D450/7f086e6acc50b53cb24944e88987c034",
			"format": "jpeg"
		}, {
			"size": 32,
			"url": "https://drscdn.500px.org/photo/165862179/q%3D80_h%3D300/bcaf3ff5e8f7fd0fb22bc18e211ff1cf",
			"https_url": "https://drscdn.500px.org/photo/165862179/q%3D80_h%3D300/bcaf3ff5e8f7fd0fb22bc18e211ff1cf",
			"format": "jpeg"
		}, {
			"size": 33,
			"url": "https://drscdn.500px.org/photo/165862179/q%3D80_h%3D600_k%3D1/cfd9d62daffb7fc5de30e190a5c1ef4a",
			"https_url": "https://drscdn.500px.org/photo/165862179/q%3D80_h%3D600_k%3D1/cfd9d62daffb7fc5de30e190a5c1ef4a",
			"format": "jpeg"
		}, {
			"size": 34,
			"url": "https://drscdn.500px.org/photo/165862179/q%3D80_m%3D1000_k%3D1/6515263ce3e2fbd42a288e28279f9818",
			"https_url": "https://drscdn.500px.org/photo/165862179/q%3D80_m%3D1000_k%3D1/6515263ce3e2fbd42a288e28279f9818",
			"format": "jpeg"
		}, {
			"size": 35,
			"url": "https://drscdn.500px.org/photo/165862179/q%3D80_m%3D1500_k%3D1/d48e6ec5ccaff39e34495b957cadb878",
			"https_url": "https://drscdn.500px.org/photo/165862179/q%3D80_m%3D1500_k%3D1/d48e6ec5ccaff39e34495b957cadb878",
			"format": "jpeg"
		}, {
			"size": 36,
			"url": "https://drscdn.500px.org/photo/165862179/q%3D80_m%3D2000_k%3D1/9ce813eb3eb1b3e4717f5b695eb9f953",
			"https_url": "https://drscdn.500px.org/photo/165862179/q%3D80_m%3D2000_k%3D1/9ce813eb3eb1b3e4717f5b695eb9f953",
			"format": "jpeg"
		}, {
			"size": 2048,
			"url": "https://drscdn.500px.org/photo/165862179/m%3D2048_k%3D1/ae57f79d32bcb15d113b5880d2ae216a",
			"https_url": "https://drscdn.500px.org/photo/165862179/m%3D2048_k%3D1/ae57f79d32bcb15d113b5880d2ae216a",
			"format": "jpeg"
		}],
		"url": "/photo/165862179/the-light-by-wacrizspiritualphoto",
		"positive_votes_count": 44,
		"converted_bits": 27,
		"share_counts": {
			"facebook": 0,
			"pinterest": 0
		},
		"tags": ["forest", "light", "vakriz", "vacriz", "foest road"],
		"watermark": true,
		"image_format": "jpeg",
		"licensing_requested": false,
		"licensing_suggested": false,
		"is_free_photo": false,
		"user": {
			"id": 8187387,
			"username": "vakriz",
			"firstname": "Wacrizspiritualphoto",
			"lastname": "",
			"city": "Pusztavacs",
			"country": "Hungary",
			"usertype": 0,
			"fullname": "Wacrizspiritualphoto",
			"userpic_url": "https://pacdn.500px.org/8187387/a4434863510e29118b63b16dfb4ee9c3fd2824b2/1.jpg?6",
			"userpic_https_url": "https://pacdn.500px.org/8187387/a4434863510e29118b63b16dfb4ee9c3fd2824b2/1.jpg?6",
			"cover_url": "https://pacdn.500px.org/8187387/a4434863510e29118b63b16dfb4ee9c3fd2824b2/cover_2048.jpg?7",
			"upgrade_status": 0,
			"store_on": true,
			"affection": 25753,
			"avatars": {
				"default": {
					"https": "https://pacdn.500px.org/8187387/a4434863510e29118b63b16dfb4ee9c3fd2824b2/1.jpg?6"
				},
				"large": {
					"https": "https://pacdn.500px.org/8187387/a4434863510e29118b63b16dfb4ee9c3fd2824b2/2.jpg?6"
				},
				"small": {
					"https": "https://pacdn.500px.org/8187387/a4434863510e29118b63b16dfb4ee9c3fd2824b2/3.jpg?6"
				},
				"tiny": {
					"https": "https://pacdn.500px.org/8187387/a4434863510e29118b63b16dfb4ee9c3fd2824b2/4.jpg?6"
				}
			},
			"followers_count": 258
		}
	}
    """
    
    assert not hasattr(iter_json, 'next'), 'Should be function that returns iterator when called, to allow restarting.'

    cc_license_names_500px = dict([(8,'CC0'),
                                   (7,'Public Domain'),
                                   (4,'CC BY'),
                                   (1,'CC NC'),
                                   (6,'CC BY-SA'),
                                   (5,'CC BY-ND'),
                                   (3,'CC BY-NC-SA'),
                                   (2,'CC BY-NC-ND'),
                                   ])
    
    for jj_top in iter_json():

        hh = {}
        
        jj = jj_top['source_record'] ## Ignore the minimal normalization we did initially. Look at source.
        
        hh['_id'] = make_id(jj_top['_id'])
        
        hh['native_id'] = jj_top['_id']
        
        hh['source_tags'] = ['500px.com']
        
        hh['source_dataset'] = '500px'

        hh['source'] = {'name':'500px',
                        'url':'https://500px.com' + jj["url"],
                        }
              
        hh['nsfw'] = jj['nsfw']

        hh['title'] = jj['name']
        
	hh['description'] = jj['description']
        
        hh['keywords'] = jj['tags']
        
        nm = cc_license_names_500px[jj['license_type']]
        
        hh['license_tags'] = [nm]
        hh['licenses'] = [{'name_long':nm, 'name':nm}]
        hh['license_url'] = None
        hh['artist_name'] = (' '.join([jj['user']['firstname'] or '', jj['user']['lastname'] or ''])).strip() or None
        
        hh['origin'] = 'https://500px.com' + jj["url"]

        hh['date_created'] = jj['created_at']
        
        st = get_image_stats(jj_top['img_data'])
        
        hh['aspect_ratio'] = st['aspect_ratio']
        
        hh['sizes'] = [{'content_type':st['mime'],      # Image mime-type
                        'width':st['width'],            # pixel width
                        'height':st['height'],          # pixel height
                        }]
        
        hh['img_data'] = jj_top['img_data']             # Data URI of this version -- only for thumbnails.
        
        yield hh
        
        
## name, func, default archive location:

normalizer_names = {'eyeem':{'func':normalize_eyeem,
                             'dir_compactsplit':'/datasets/datasets/compactsplit/eyeem',
                             'dir_cache':'/datasets/datasets/eyeem/images/',
                             },
                    'getty_archiv':{'func':normalize_getty,
                                    'dir_compactsplit':'/datasets/datasets/compactsplit/getty_archiv',
                                    'dir_cache':'/datasets2/datasets/getty_unpack/getty_archiv/downloads/comp/',
                                    },
                    'getty_rf':{'func':normalize_getty,
                                'dir_compactsplit':'/datasets/datasets/compactsplit/getty_rf',
                                'dir_cache':'/datasets2/datasets/getty_unpack/getty_rf/downloads/comp/',
                                },
                    'getty_entertainment':{'func':normalize_getty,
                                           'dir_compactsplit':'/datasets/datasets/compactsplit/getty_entertainment',
                                           'dir_cache':'/datasets2/datasets/getty_unpack/getty_entertainment/downloads/comp/',
                                           },
                    'dpla':{'func':normalize_dpla,
                            'dir_compactsplit':'/datasets/datasets/compactsplit/dpla',
                            'dir_cache':'/datasets/datasets/dpla/images/',
                            },
                    'mirflickr1mm':{'func':normalize_mirflickr1mm,
                                    'dir_compactsplit':'/datasets/datasets/compactsplit/mirflickr1mm',
                                    'dir_cache':False,
                                    },
                    'pexels':{'func':normalize_pexels,
                              'dir_compactsplit':'/datasets/datasets/compactsplit/pexels',
                              'dir_cache':'/datasets/datasets/pexels/images_1920_1280/',
                              },
                    'places':{'func':normalize_places,
                              'dir_compactsplit':'/datasets/datasets/compactsplit/places',
                              'dir_cache':False,
                              },
                    '500px':{'func':normalize_500px,
                              'dir_compactsplit':'/datasets/datasets/compactsplit/500px',
                              'dir_cache':False,
                             },
                    }


def apply_normalizer(iter_json,
                     normalizer_name,
                     ):
    """
    Apply the normalizer of a given name.
    """

    assert not hasattr(iter_json, 'next'), 'Should be function that returns iterator when called, to allow restarting.'

    func = normalizer_names[normalizer_name]['func']

    for c, x in enumerate(func(iter_json)):

        if c <= 50:
            simple_schema_validate(x)
        
        yield x


def apply_post_ingestion_normalizers(rr,
                                     schema_variant = 'old',
                                     ):
    """
    Post-ingestion normalizers that are applied last-moment at indexer query time.
    
    TEMPORARY - 
        Currently only temporary code lives here, as it is all already done in the pre-ingestion
        normalizers. When the datasets are re-generated, the following functions can be removed.
        Put here as a JIT transformation because regenerating datasets is slow.
    
    TODO - 
        Do both pre-ingestion and post-ingestion normalization?
    """

    print ('schema_variant',schema_variant)
    
    for ii in rr:
        native_id = ''
        try:
            native_id = ii['_source']['native_id']
        except:
            ## Likely images that didn't go through the mc_normalizers path and don't have `native_id`s.
            pass

        if native_id.startswith('pexels'):
            ii['_source']['title'] = None

        ## add permalinks here:

        if 'getty_' in native_id:
            ii['_source']['source']['url'] = 'http://www.gettyimages.com/detail/photo/permalink/' + native_id.replace('getty_','')
            
        if False:#'pexels_' in native_id:
            ii['_source']['source']['url'] = ii['url_shown_at']['url']

        ## license stuff:
        
        if 'pexels_' in native_id:
            source_tags = ['pexels.com']
        
            if ii['_source'].get('source',{}).get('name'):
                source_tags.append(ii['_source']['source']['name'])
            
            source_tags = list(set([(x[len('https://'):] if x.startswith('https://') else x) for x in source_tags]))
            source_tags = list(set([(x[len('http://'):] if x.startswith('http://') else x) for x in source_tags]))
            source_tags = list(set([(x[len('www.'):] if x.startswith('www.') else x) for x in source_tags]))
            
            ii['_source']['source_tags'] = source_tags
            
            ii['_source']['license_tags'] = ['CC0']
            ii['_source']['license_name'] = "CC0"
            ii['_source']['license_name_long'] = "Creative Commons Zero (CC0)"
            ii['_source']['license_url'] = None
            ii['_source']['license_attribution'] = ii.get('artist_names') and ', '.join(ii['artist_names']) or None
            
        if 'getty_' in native_id:
            ii['_source']['source_tags'] = ['gettyimages.com']
            
            ii['_source']['license_tags'] = ['Non-Commercial Use']
            ii['_source']['license_name'] = "Getty Embed"
            ii['_source']['license_name_long'] = "Getty Embed"
            ii['_source']['license_url'] = "http://www.gettyimages.com/Corporate/LicenseAgreements.aspx#RF"
            ii['_source']['license_attribution'] = ii.get('artist_names') and ', '.join(ii['artist_names']) or None
            
        if 'eyeem_' in native_id:

            ## TODO - fake sizes, remove when re-ingestion is complete:

            assert 'sizes' in ii['_source'],repr(ii['_source'])
            
            if 'sizes' not in ii['_source']:
                
                sizes = [{'width':1920,                   # fake width
                          'height':1280,                  # fake height
                          'dpi':None,                     # DPI - Use to estimate real-world width / height if needed?
                          'bytes':None,                   # bytes size of this version
                          'content_type':'image/jpeg',    # Image mime-type
                          'uri_external':None,            # External URI.
                          }
                         ]

                ii['_source']['sizes'] = sizes
        
        if schema_variant == 'new':
            ## New Schema Format
            ## See: https://rawgit.com/mediachain/mediachain-indexer/master/doc/index.html

            try:
                if ii['_source'].get('artist_names') and (type(ii['_source'].get('artist_names')[0]) == list):
                    ii['_source']['artist_name'] = ', '.join(ii['_source']['artist_names'][0])
                else:
                    ii['_source']['artist_name'] = ', '.join(ii['_source']['artist_names']) if ii['_source'].get('artist_names') else None
            except:
                print repr(ii['_source'].get('artist_names'))
                ii['_source']['artist_name'] = None
            
            ii['_source']['date_created'] = ii['_source'].get('date_created_original') or ii['_source'].get('date_created_at_source') or None

            if ii['_source']['title'] and ii['_source']['title'][0]:
                
                if isinstance(ii['_source']['title'], basestring):
                    ii['_source']['title'] = ii['_source']['title']
                else:
                    ii['_source']['title'] = ' '.join(ii['_source']['title'])
            
            else:
                ii['_source']['title'] = None
            
            if ii['_source'].get('licenses'):
                                
                ii['_source']['license'] = ii['_source']['licenses'][0]
                ii['_source']['license']['url'] = ii['_source'].get('license_url')

                if ii['_source']['license'].get('name') == 'CC0':
                    ii['_source']['license']['url'] = 'https://creativecommons.org/publicdomain/zero/1.0/'
                    
                if ii['_source']['license'].get('name_long') == 'Getty Embed':
                    ii['_source']['license']['url'] = 'http://www.gettyimages.com/company/terms'
                
            else:
                ii['_source']['license'] = None
            
            if ii['_source'].get('url_shown_at',{}).get('url'):
                ii['_source']['origin'] = {'url': ii['_source']['url_shown_at']['url']}
                ii['_source']['origin']['name'] = urlparse.urlsplit(ii['_source']['url_shown_at']['url']).netloc
            else:
                ii['_source']['origin'] = None
            
            ii['_source']['image_url'] = ii['_source']['url_direct_cache']['url']

            ## Blockchain getty stuff:
            
            if ii['_source'].get('artist') and (not ii['_source']['artist_name']):
                ii['_source']['artist_name'] = ii['_source']['artist']

            ## TODO - license for blockchain getty.
            
            ## Delete superseded:
            
            for kk in ['artist_names', 'date_created_original', 'date_created_at_source', 'licenses',
                       'url_shown_at', 'url_direct_cache',
                       ]:
                if kk in ii['_source']:
                    del ii['_source'][kk]


        
def get_type_str(x):
    s = str(type(x))
    assert "<type '" in s,repr(s)
    
    r =  'TYPE=' + s.replace("<type '",'').replace("'>",'')

    r = r.upper()
    
    if r == 'TYPE=STR':
        return 'TYPE=UNICODE'
    
    if r == 'TYPE=NONETYPE':
        return 'TYPE=NULL'
    
    return r


def walk_json_shapes_types(hh, path = [], sort = True, leaves_as_types = True, include_falsy_leaves = True):
    """
    hh = {'z':{'a': '123',
               'b': {'url': 234},
               'c': [{'url': '567'}, 
                     {'url': '8910'},
                    ],
              }
         }
    
    walk_json_shapes_types(hh)
    
    -> [('TYPE=DICT', 'z', 'TYPE=DICT', 'a', 'TYPE=UNICODE'),
        ('TYPE=DICT', 'z', 'TYPE=DICT', 'b', 'TYPE=DICT', 'url', 'TYPE=INT'),
        ('TYPE=DICT', 'z', 'TYPE=DICT', 'c', 'TYPE=LIST', 'TYPE=DICT', 'url', 'TYPE=UNICODE'),
        ('TYPE=DICT', 'z', 'TYPE=DICT', 'c', 'TYPE=LIST', 'TYPE=DICT', 'url', 'TYPE=UNICODE')]

    walk_json_shapes_types({1:{2:{3:4}}}, leaves_as_types = False)

    -> [('TYPE=DICT', 1, 'TYPE=DICT', 2, 'TYPE=DICT', 3, 4)]
    
    """
        
    path = path[:]
    
    v = hh
    
    if type(v) == dict:

        if include_falsy_leaves:
            if not v:
                yield path + [get_type_str(v) if leaves_as_types else v]
        
        zz = v.iteritems()
    
        if sort:
            zz = sorted(zz)
        
        for kk, vv in zz:            
            for xx in walk_json_shapes_types(vv,
                                             path + [get_type_str(v)] + [kk],
                                             leaves_as_types = leaves_as_types,
                                             ):
                yield xx

    elif hasattr(v, '__iter__'):
        
        if include_falsy_leaves:
            if not v:
                yield path + [get_type_str(v) if leaves_as_types else v]
                
        for xx in v:
            for yy in walk_json_shapes_types(xx,
                                             path + [get_type_str(v)],
                                             leaves_as_types = leaves_as_types,
                                             ):
                yield yy

    else:
        yield tuple(path + [get_type_str(v) if leaves_as_types else v])



def reproduce_json_from_shapes(path_list, verbose = False):
    """
    Stack-based rebuilding of flattened json shapes.
    """

    path_list = list(path_list)
    
    lookup = {('TYPE=DICT', 'ROOT'):[]}      ## {path:obj}

    ## Flatten and add padding:
    
    path_list = [['TYPE=DICT'], ['TYPE=DICT', 'ROOT']] + path_list

    done = set()
    rr = []
    for c,path in enumerate(path_list):
        path = list(path)
        if c > 1:
            path = ['TYPE=DICT', 'ROOT'] + path

        for x in xrange(1, len(path) + 1):

            path = [(tuple(z) if type(z) == list else z) for z in path]
            
            y = tuple(path[:x])
            if repr(y) not in done:
                rr.append(y)
            done.add(repr(y))
    path_list = rr

    if verbose:
        print '===='
        for x in path_list:
            print x
        print '===='

    ## Main loop:
        
    for c,path in enumerate(path_list):

        path = list(path)
        
        #print 'path',path
        
        ## CREATE OBJECTS:
        
        cur_node = path[-1]
        
        if tuple(path) not in lookup:
            
            if cur_node == 'TYPE=TUPLE':
                lookup[tuple(path)] = [] ## switch to tuple later

            elif cur_node == 'TYPE=DICT':
                lookup[tuple(path)] = {}

            elif cur_node == 'TYPE=LIST':
                lookup[tuple(path)] = []
            else:
                assert not (isinstance(cur_node, basestring) and cur_node.startswith('TYPE=')), cur_node
                lookup[tuple(path)] = cur_node
                assert cur_node != 'TYPE=DICT'

        if len(path) > 2:
            ## APPEND OBJECTS DEFINED VIA 1 LAYER:
            
            prev_node = path[-2]

            if prev_node == 'TYPE=TUPLE':
                lookup[tuple(path[:-1])].append(lookup[tuple(path)])
                assert lookup[tuple(path)] != 'TYPE=DICT'
                
            elif prev_node == 'TYPE=LIST':
                lookup[tuple(path[:-1])].append(lookup[tuple(path)])
                assert lookup[tuple(path)] != 'TYPE=DICT'

            elif prev_node == 'TYPE=DICT':
                pass

            else:
                #probably partially-built dict
                pass

 
            ## APPEND OBJECTS DEFINED VIA 2 LAYERS:

            prev_node_2 = path[-3]
            
            if prev_node_2 == 'TYPE=DICT':
                #print 'ASSIGN',tuple(path[:-2]),'--',prev_node,'--',cur_node
                lookup[tuple(path[:-2])][prev_node] = lookup[tuple(path)]
        
        #print '->this',lookup[tuple(path)]
    
    return lookup[('TYPE=DICT',)]['ROOT']

    

def dump_example_schemas(top_num = 50,
                         max_num = 10000,
                         max_leaf_string_length = 200,
                         via_cli = False,
                         ):
    """
    Create schema reports, including example schemas, from all compactsplit formatted datasets.
    
    Args:
        top_num:                 Show top `top_num` examples for each field.
        max_num:                 Number of documents to sample from each dataset. 0 for all documents.
        max_leaf_string_length:  Cut field value strings that are longer than this length, for reading
                                 brevity. 0 to ignore.
    """
    

    def special_repr(x):
        x = repr(x)
        if x == 'True':
            x = 'true'
        elif x == 'False':
            x = 'false'
        elif x == 'None':
            x = 'null'
        return x
    
    from random import choice
    from mc_datasets import iter_compactsplit
    from collections import Counter
    import json
    from ast import literal_eval
    
    all_common_examples = {}

    longest_paths = {} ## {short:long}
    
    for c,(name, nh) in enumerate(normalizer_names.iteritems()):
        
        print ('START', name)
        
        func = nh['func']
        fn = nh['dir_compactsplit']
        
        for cc,rr in enumerate(func(lambda : iter_compactsplit(fn, max_num = max_num))):
            
            #print 'RR',c,cc,repr(rr)
            
            type_paths = list(walk_json_shapes_types(rr, leaves_as_types = False))
            
            for ccc,type_path in enumerate(type_paths):
                
                #print (c,cc,ccc,'type_path',type_path)
                
                #type_path = [repr(x) for x in type_path]

                #if 'artist_names' in repr(type_path):
                #    print type_path
                
                pth = tuple(type_path[:-1])
                leaf = type_path[-1]

                if max_leaf_string_length:
                    if isinstance(leaf, basestring) and (len(leaf) > max_leaf_string_length):
                        leaf = leaf[:max_leaf_string_length] + '...[CUT]'

                leaf = repr(leaf) ## reversed later with literal_eval
                    
                if pth not in all_common_examples:
                    all_common_examples[pth] = Counter()
                all_common_examples[pth][leaf] += 1
                
                if len(all_common_examples[pth]) > 200:
                    all_common_examples[pth] = Counter(dict(all_common_examples[pth].most_common(50)))

                for x in xrange(1, len(pth) + 1):
                    short = pth[:x]

                    if len(longest_paths.get(short,[])) < len(pth):
                        longest_paths[short] = pth


    ## Now we've ignored short circuits caused by falsy types:
    
    rt = []
    ro = []
    for path in sorted(longest_paths.values()):

        ## Show the top `top_num` examples, ignoring `null` unless it's the only option:
        
        zz = [special_repr(literal_eval(x)) for x,y in all_common_examples[path].most_common(top_num + 1)]

        zz = [x for x in zz if x != 'null'][:top_num]

        ex = 'EXAMPLES(' + (', '.join(zz)) + ')'

        path_t = list(path) + [ex]
        
        print 'EX',path_t
        
        rt.append(path_t)

        ## for top-1, choose randomly one of the top 5: 
        
        bb = [literal_eval(x) for x,y in all_common_examples[path].most_common(5) if x is not None]

        path_o = list(path) + [choice(bb) if bb else None]

        ro.append(path_o)

    with open('/datasets/datasets/schema_example_top_%d.json' % top_num,'w') as ft:
         ft.write(json.dumps(reproduce_json_from_shapes(rt), indent=4))
                  
    with open('/datasets/datasets/schema_example_single.json','w') as fo:
        fo.write(json.dumps(reproduce_json_from_shapes(ro), indent=4))

    print json.dumps(reproduce_json_from_shapes(rr), indent=4)


def test_normalizers(max_num = 100,
                     dump_schemas = True,
                     exception_on_type_change = True,
                     exception_on_byte_strings = False,
                     via_cli = False,
                     ):
    """
    Tests for:
        1) Differences in record tree shapes / types for all records from the SAME normalizer.
        2) Differences in record tree shapes / types for all records from ALL normalizers.
    
    Tests for, and raises exceptions on:
        3) Exceptions thrown by the normalizer functions.
        4) (exception_on_type_change) Presence of more than 1 type used for any node (excluding
           null, and sometimes `[]` or `{}`).
        5) (exception_on_byte_strings) Any non-unicode strings. TODO.
    """
    
    with open('/datasets/datasets/schemas_all_paths.txt','w') as f_out:
        
        from mc_datasets import iter_compactsplit
        from collections import Counter
        
        all_common_examples = {}
        
        all_common_all = set()
        all_common_any = set()
        all_common_schema = {} #{path:[type, ...], ...}
        all_counts = Counter()
        
        for c,(name, nh) in enumerate(normalizer_names.iteritems()):

            print ('START', name)

            func = nh['func']
            fn = nh['dir_compactsplit']

            this_common_all = set()
            this_common_any = set()
            this_common_schema = {} #{path:[type, ...], ...}

            counts = Counter()

            nn = 0
            for cc,rr in enumerate(func(lambda : iter_compactsplit(fn, max_num = max_num))):
                nn += 1

                #print 'RR',c,cc,repr(rr)

                type_paths = list(walk_json_shapes_types(rr))

                paths = [tuple(x[:-1]) for x in type_paths]

                #print 'paths',paths
                #raw_input_enter()

                if c == 0:
                    all_common_all.update(paths)
                if cc == 0:
                    this_common_all.update(paths)

                this_common_all.intersection_update(paths)
                this_common_any.update(paths)
                all_common_all.intersection_update(paths)
                all_common_any.update(paths)

                for ccc,type_path in enumerate(type_paths):

                    #print (c,cc,ccc,'type_path',type_path)

                    pth = tuple(type_path[:-1])
                    leaf = type_path[-1]

                    assert 'TYPE=' in leaf,type_path

                    counts[pth] += 1
                    all_counts[pth] += 1

                    if pth not in all_common_schema:
                        all_common_schema[pth] = []

                    if leaf not in all_common_schema[pth]:
                        all_common_schema[pth].append(leaf)

                    if pth not in this_common_schema:
                        this_common_schema[pth] = []

                    if leaf not in this_common_schema[pth]:
                        this_common_schema[pth].append(leaf)

                    if pth not in all_common_examples:
                        all_common_examples[pth] = Counter()

                    all_common_examples[pth][(repr(leaf), get_type_str(leaf))] += 1
                    
                    

                #raw_input_enter()
            print
            print ('====COMMON_PATHS:',name)
            for xx in sorted(this_common_all):
                print space_pad(counts[xx],6),xx
            print

            print ('====DIFFS:',name)
            for xx in sorted(this_common_any.difference(this_common_all)):
                print space_pad(counts[xx],6),xx
            print


            print ('====COMMON_SCHEMA',name)
            max_k = max([len(unicode(k)) for k,v in this_common_schema.items()])
            for k,v in sorted(this_common_schema.items()):
                print space_pad(k,max_k,ch=' '),v

                #raw_input_enter()
                
            print ('DONE',name, nn)

        print
        print '====ALL_COMMON_SCHEMA'
        ii = [(' -> '.join(k),v) for k,v in all_common_schema.items()]
        max_k = max([len(k) for k,v in ii]) + 1
        f_out.write(space_pad('SCHEMA_PATH',max_k,ch=' ') + 'LEAF_TYPE(S)' + '\n')
        for k,v in sorted(ii):
            print space_pad(k,max_k,ch=' '),u', '.join(v)
            f_out.write((space_pad(k,max_k,ch=' ') + unicode(u', '.join(v))).encode('utf8') + '\n')
        print

        #print '====ALL_DIFFS:'
        #for xx in sorted(all_common_any.difference(all_common_all)):
        #    print space_pad(all_counts[xx],6),xx
        #print
        
        print
        print '====ALL_COMMON_EXAMPLES'        
        ii = [(' -> '.join(k),v) for k,v in all_common_schema.items()]
        max_k = max([len(k) for k,v in ii]) + 1
        f_out.write(space_pad('SCHEMA_PATH',max_k,ch=' ') + 'LEAF_TYPE(S)' + '\n')
        for k,v in sorted(ii):
            print space_pad(k,max_k,ch=' '),u', '.join(v)
            f_out.write((space_pad(k,max_k,ch=' ') + unicode(u', '.join(v))).encode('utf8') + '\n')
        print
        
        print ('DONE_ALL')



    
def dump_normalized_schemas(dir_in = '/datasets/datasets/compactsplit/',
                            fn_out = '/datasets/datasets/schemas_normalized.js',
                            via_cli = False,
                            ):
    """
    Dump normalized schemas. Note: DPLA has 1500+, and we're only showing the first few.
    """

    from os import mkdir, listdir, makedirs, walk
    import json
    from gzip import GzipFile
    from os.path import join, exists, split
    import sys

    seen_providers = set()
    
    with open(fn_out, 'w') as f_out:

        for name, nh in normalizer_names.iteritems():

            func = nh['func']
            d2 = nh['dir_compactsplit']
            
            nm = split(d2)[-1]

            fn = list(sorted(listdir(d2)))[0]
            
            fn = join(d2, fn)
            
            if 'entertainment' in fn:
                continue

            if 'archiv' in fn:
                continue

            if fn.endswith('gz'):
                ff = GzipFile(fn, 'r')
            else:
                ff = open(fn)

            def the_iter():
                for line in ff:
                    yield json.loads(line[line.index('\t') + 1:])
                
            for cc,hh in enumerate(func(the_iter)):
                
                if False:#'dpla' in fn:
                    if len(seen_providers) == 20:
                        break

                    #if cc == 100000:
                    #    break
                    try:
                        prov = hh['source_record']['_source']['dataProvider']
                    except:
                        prov = hh['source_record']['_source']['provider']


                    if not seen_providers:
                        common_all = set(hh['source_record']['_source'].keys())
                        common_any = set(hh['source_record']['_source'].keys())
                    else:
                        common_all.intersection_update(hh['source_record']['_source'].keys())
                        common_any.update(hh['source_record']['_source'].keys())

                    #print ('SOURCE_KEYS',hh['source_record']['_source'].keys())


                    if (type(prov) == list):
                        prov = ' && '.join(prov)

                    elif (type(prov) == dict) and ('name' in prov):
                        prov = prov['name']

                    nm = 'dpla' + ' -- ' + prov.encode('utf8')

                    if prov in seen_providers:
                        continue
                    print ('DPLA_NEW_PROVIDER',cc,'seen_providers:',len(seen_providers),prov)
                    seen_providers.add(prov)

                f_out.write('// SCHEMA: ' + nm + ':\n\n')

                del hh['img_data']

                zz = hh
                #zz = hh['source_record']

                f_out.write(json.dumps(zz,indent=4) + '\n\n')

                if True:#'dpla' not in fn:
                    break

    try:
        print ('DPLA_COMMON_ALL',common_all)
        print ('DPLA_COMMON_SOME',common_any.difference(common_all))
    except:
        pass

    print 'DONE',fn_out
                
    
from mc_generic import setup_main, raw_input_enter, space_pad

functions=['test_normalizers',
           'dump_normalized_schemas',
           'dump_example_schemas',
           ]

def main():

    setup_main(functions,
               globals(),
                'mediachain-indexer-translate',
               )

if __name__ == '__main__':
    main()
