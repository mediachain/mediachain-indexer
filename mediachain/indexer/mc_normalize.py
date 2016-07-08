#!/usr/bin/env python

"""
Last-moment normalization of records.

Future:
- cache these?
- multi-pass supervised ML methods?
- refine difference between normalizer vs translator?
"""


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
import Image
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
    return h

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
        
        hh = {'_id':jj_top['_id'],
              'img_data':jj_top['img_data'],                # Data URI of this version -- only for thumbnails.
              'artist_names':[jj['artist']],       # Simple way to access artist name(s).
              'source_name':'getty',
              'title':[jj['title']],               # Title string(s)
              'attribution':[{                     ## Full attribution details, for when "artist" isn't quite the right description.
                  'role':'artist',                 # Contribution type.
                  'details':None,                  # Contribution details. 
                  'name':None,                     # Entity name.
                  }],
              'keywords':[x['text'] for x in jj['keywords'] if 'text' in x], # Keywords
              'orientation':jj['orientation'],                # Should photo be rotated?
              'editorial_source_name':jj['editorial_source'], #
              'editorial_source':{
                  'name':jj['editorial_source'],              #
                  },
              'date_created_original':None,                 # Actual creation date.
              'date_created_at_source':jj['date_created'],    # Item created at data source.
              'camera_exif':{},                    # Camera Exif data
              'licenses':[                         ## List of licenses:
                  {'license_name':None,            # License name
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
              'sightings':[{}],          ## TODO - Sightings list.
              'mediachain_record':None,  # Mediachain record.
              }
        
        yield hh



        
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

        mime = get_image_stats(jj_top['img_data'])['mime']
        
        sizes = [{'width':1920,                   # pixel width
                  'height':1280,                  # pixel height
                  'dpi':None,                     # DPI - Use to estimate real-world width / height if needed?
                  'bytes':None,                   # bytes size of this version
                  'content_type':mime,            # Image mime-type
                  'uri_external':None,            # External URI.
                  }
                 ]
        
        hh = {'_id':jj_top['_id'],
              'img_data':jj_top['img_data'],  # Data URI of this version -- only for thumbnails.
              'artist_names':None,
              'source_name':'pexels',                         #
              'date_source_version':None,                     # Date this snapshot started
              'date_captured':None,
              'date_created_original':None,                   # Actual creation date.
              'date_created_at_source':None,                  # Item created at data source.
              'title':' '.join(jj['the_kw']),                 # Title string(s)
              'attribution':[],                               ## Artist / Entity names.
              'keywords':jj['the_kw'],                        # Keywords
              'orientation':None,                             # Should photo be rotated?
              'editorial_source_name':None,                   #
              'editorial_source':None,
              'camera_exif':{},                               # Camera Exif data
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
              'sightings':[{             ## TODO - Sightings list.
                  'url':jj['the_canon'],
                  }],
              'mediachain_record':None,  # Mediachain record.
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
        "shown_at_url": "http://dc.lib.unc.edu/u?/ncmaps,6766", 
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
        artist_names = None
        try:
            artists = [{'name':x} for x in jj['sourceResource']['creator']]
            artist_names = ', '.join([x['name'] for x in artists])
        except:
            pass
        
        hh = {'_id':jj_top['_id'],
              'img_data':jj_top['img_data'],  # Data URI of this version -- only for thumbnails.
              'artist_names':artist_names,
              'provider_chain':[                              ## List of providers. Top most recent.
                  {'name':'dpla'},
                  {'name':jj['provider']['name']},
                  ],
              'sightings_chain':[                             ## List of sightings, top most recent.
                  {'url_page':jj['isShownAt'],
                  'url_image':jj['object'],
                  }
                  ],
              'date_source_version':None,                     # Date this snapshot started
              'date_captured':None,
              'date_created_original':get_shallowest_matching(jj, 'displayDate'), # Actual creation date.
              'date_created_at_source':None,                  # Item created at data source.
              'title':get_shallowest_matching(jj, 'title'),    # Title string(s)
              'description':get_shallowest_matching(jj, 'description'), # Description
              'attribution':artists,                          ## Artist / Entity names.
              'keywords':[],                                  # Keywords
              'orientation':None,                             # Should photo be rotated?
              'editorial_source_name':None, #
              'editorial_source':None,
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
                  'general_type':get_shallowest_matching(jj, 'format'),   # (photo, illustration, GIF, face)
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
              'mediachain_record':None,  # Mediachain record.
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
        
        sizes = [{'width':st['width'],            # pixel width - TODO
                  'height':st['height'],          # pixel height - TODO
                  'dpi':None,                     # DPI - Use to estimate real-world width / height if needed?
                  'bytes':None,                   # bytes size of this version
                  'content_type':st['mime'],      # Image mime-type
                  'uri_external':jj['license'].get('Web url'),    # External URI.
                  }
                 ]
        
        hh = {'_id':jj_top['_id'],
              'img_data':jj_top['img_data'],  # Data URI of this version -- only for thumbnails.
              'artist_names':['license'].get("Owner name"),
              'provider_chain':[                              ## List of providers, most recent first.
                  {'name':'flickr'},
                  ],
              'sightings_chain':[                             ## List of sightings, most recent first.
                  {'url_page':jj['license'].get('Photo url'),
                   'url_image':jj['license'].get('Web url'),
                  }
                  ],
              'date_source_version':None,                     # Date this snapshot started
              'date_captured':get_shallowest_matching(jj, "-Date and Time"),
              'date_created_original':get_shallowest_matching(jj, "-Date and Time"),  # Actual creation date.
              'date_created_at_source':jj['license']['Date uploaded'],      # Item created at data source.
              'title':jj['license'].get("Picture title"),    # Title string(s)
              'description':None,                        # Description
              'attribution':[                            ## Artist / Entity names.
                  {'name':jj['license'].get("Owner name"),
                   'role':'artist',
                  }
                  ],                               
              'keywords':[],                                  # Keywords
              'orientation':None,                             # Should photo be rotated?
              'editorial_source_name':None, #
              'editorial_source':None,
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
              'mediachain_record':None,  # Mediachain record.
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
        
        hh = {'_id':jj_top['_id'],
              'img_data':jj_top['img_data'],  # Data URI of this version -- only for thumbnails.
              'artist_names':None,
              'provider_chain':[{'name':'places'}],      ## List of providers, most recent first.
              'sightings_chain':[],                      ## List of sightings, most recent first.
              'date_source_version':None,                # Date this snapshot started
              'date_captured':None,
              'date_created_original':None,              # Actual creation date.
              'date_created_at_source':None,             # Item created at data source.
              'title':[jj_top["title"]],                 # Title string(s)
              'description':None,                        # Description
              'attribution':[],                          ## Artist / Entity names.
              'keywords':[],                                  # Keywords
              'orientation':None,                             # Should photo be rotated?
              'editorial_source_name':None, #
              'editorial_source':None,
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
              'mediachain_record':None,  # Mediachain record.
              }
        
        yield hh


## name, func, default archive location:

normalizer_names = {'getty_archiv':(normalize_getty, '/datasets/datasets/compactsplit/getty_archiv'),
                    'getty_rf':(normalize_getty, '/datasets/datasets/compactsplit/getty_rf'),
                    'getty_entertainment':(normalize_getty, '/datasets/datasets/compactsplit/getty_entertainment'),
                    'dpla':(normalize_dpla, '/datasets/datasets/compactsplit/dpla'),
                    'mirflickr1mm':(normalize_mirflickr1mm, '/datasets/datasets/compactsplit/mirflickr1mm'),
                    'pexels':(normalize_pexels, '/datasets/datasets/compactsplit/pexels'),
                    'places':(normalize_places, '/datasets/datasets/compactsplit/places'),
                    }


def apply_normalizer(iter_json,
                     normalizer_name,
                     ):
    """
    Apply the normalizer of a given name.
    """
    
    assert not hasattr(iter_json, 'next'), 'Should be function that returns iterator when called, to allow restarting.'
    
    func, _ = normalizer_names[normalizer_name]
    
    for x in func(iter_json):
        yield x


def test_normalizers(via_cli = False):
    """
    """
    
    from mc_datasets import iter_compactsplit
        
    for name, (func, fn) in normalizer_names.iteritems():
        print ('START', name)
        
        nn = 0
        for _ in func(lambda : iter_compactsplit(fn, max_num = 100)):
            nn += 1
        
        print ('DONE',name, nn)
    
    print ('DONE_ALL')


functions=['test_normalizers',
           ]

def main():
    from mc_generic import setup_main
    
    setup_main(functions,
               globals(),
                'mediachain-indexer-translate',
               )

if __name__ == '__main__':
    main()

