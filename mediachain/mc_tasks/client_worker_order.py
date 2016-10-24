#!/usr/bin/env python

def setup_extract_10crop(gpu_id = 0,
                         vgg19_proto_path = '/zdrive/order-embedding/VGG_ILSVRC_19_layers_deploy.prototxt',
                         caffe_path = '/zdrive/order-embedding/VGG_ILSVRC_19_layers.caffemodel',
                         ):
    """
    Adapted from: https://github.com/ivendrov/order-embedding/blob/master/extract_cnn_features.py
    """
    
    import caffe
    import json
    import numpy
    from collections import defaultdict
    import sklearn.preprocessing
    from PIL import ImageFile
    import os
    import numpy as np

    ImageFile.LOAD_TRUNCATED_IMAGES = True  # needed for coco train

    net_name = 'VGG19'
    
    cnns = {
        'VGG19':
        {
            'prototxt': vgg19_proto_path,
            'caffemodel': caffe_path,
            'features_layer': 'fc7',
            'mean': numpy.array([103.939, 116.779, 123.68])  # BGR means, from https://gist.github.com/ksimonyan/3785162f95cd2d5fee77
        }
    }
    
    net_data = cnns[net_name]
    layer = net_data['features_layer']
    
    # load caffe net
    caffe.set_mode_gpu()
    caffe.set_device(gpu_id)
    net = caffe.Net(net_data['prototxt'], net_data['caffemodel'], caffe.TEST)
    batchsize, num_channels, width, height = net.blobs['data'].data.shape

    # set up pre-processor
    transformer = caffe.io.Transformer({'data': net.blobs['data'].data.shape})

    transformer.set_transpose('data', (2,0,1))
    transformer.set_channel_swap('data', (2,1,0))
    transformer.set_mean('data', net_data['mean'])
    transformer.set_raw_scale('data', 255)

    
    def extract_10crop(filenames):
        ## https://github.com/ivendrov/order-embedding/blob/master/extract_cnn_features.py
        """ Extracts CNN features
        :param filenames: list of filenames for images
        :param output_dir: the directory to store the features in
        :param gpu_id: gpu ID to use to run computation
        """        
        
        feat_shape = [len(filenames)] + list(net.blobs[layer].data.shape[1:])
        print("Shape of features to be computed: " + str(feat_shape))
        
        feats = {}
        for key in ['10crop']: #,'1crop'
            feats[key] = numpy.zeros(feat_shape).astype('float32')
        
        for k in range(len(filenames)):
            print('Image %i/%i' % (k, len(filenames)))
            im = caffe.io.load_image(filenames[k])
            h, w, _ = im.shape
            if h < w:
                im = caffe.io.resize_image(im, (256, 256*w/h))
            else:
                 im = caffe.io.resize_image(im, (256*h/w, 256))
            
            crops = caffe.io.oversample([im], (width, height))
            
            for i, crop in enumerate(crops):
                net.blobs['data'].data[i] = transformer.preprocess('data', crop)
            
            n = len(crops)
            
            net.forward()
            
            output = net.blobs[layer].data[:n]
            
            for key, f in feats.items():
                output = numpy.maximum(output, 0)

                if key == '10crop':
                    f[k] = output.mean(axis=0)  # mean over 10 crops
                else:
                    f[k] = output[4]  # just center crop
        
        print("Saving features...")
        
        rr = {x:[] for x in feats}
        
        for methodname, f in feats.items():
            f = sklearn.preprocessing.normalize(f)
            rr[methodname] = f
        
        return rr['10crop']
    
    return extract_10crop


def client_worker_order(qq_input,
                        qq_output,
                        qq_shutdown,
                        TASK_ID,
                        ):
    assert TASK_ID in ['order_model', 'order_model_2', 'order_model_3', 'order_model_3_oneoff'], (TASK_ID,)

    is_oneoff = TASK_ID.endswith('_oneoff') ## accepts new tasks over http, store in different cache dir.
    
    order_model_path = VALID_TASKS[TASK_ID]['order_model_path']
    
    print ('USING_MODEL', TASK_ID, order_model_path)
    sleep(2)
    
    assert exists(order_model_path + '.npz'), order_model_path + '.npz'
    assert exists(order_model_path + '.pkl'), order_model_path + '.pkl'
    
    from order_embedding import tools, evaluation
    
    import json
    import numpy
    import os
    import numpy as np
    
    extract_10crop = setup_extract_10crop()
    
    #####
    
    order_model = tools.load_model(order_model_path)
    
    #Where im is a NumPy array of VGG features. Note that the VGG features were scaled to unit norm prior to training the models:
    
    #######
    while True:
        try:
            batch = qq_input.get(timeout = 1)
        except:
            print ('CLIENT_WORKER_ORDER_MODEL_GET_QUEUE_TIMEOUT',current_thread().name)
            continue

        print ('GOT_BATCH', batch['batch_id'], len(batch))

        expected_batch_size = len(batch['batch'])

        start_t = time()

        fns = []
        for rec in batch['batch']:
            
            ff = NamedTemporaryFile(delete = False,
                                    prefix = 'order_',
                                    suffix = '.jpg'
                                    )
            rec['fn'] = ff.name
            ff.write(rec['data'])
            ff.flush()
            ff.close()
            fns.append(ff.name)
        
        if False:
            feats_out = extract_10crop([x['fn'] for x in batch['batch']])
        
        else:
            ## Cache these vgg19s finally:
            
            feats_out = []
            for rec in batch['batch']:
                fn2 = get_fn_out(rec['_id'], 'vgg19', 'w')
                got_it = False
                if exists(fn2):
                    print ('LOAD', fn2)
                    try:
                        yy = np.load(fn2, mmap_mode='r')
                        got_it = True
                    except:
                        print ('BAD_FILE', fn2)
                        unlink(fn2)
                if not got_it:
                    yy = extract_10crop([rec['fn']])
                    with open(fn2, 'w') as f:
                        np.save(f, yy)       
                feats_out.append(yy[0])
            feats_out = np.array(feats_out)
        
        print ('GOT_FEATS', feats_out.shape)
                        
        feats_out = list(feats_out)

        assert batch['task_id'] == TASK_ID, (batch['task_id'], TASK_ID)
        
        rr = []
        for rec, im in zip(batch['batch'], feats_out):
            
            #sentence_vectors = tools.encode_sentences(order_model, sentences, verbose=True)
            
            #print ('im.shape',im.shape)

            #print ('ARRAY', numpy.array([im]))
            
            image_vectors = tools.encode_images(order_model, numpy.array([im]))
            
            #print ('image_vectors',image_vectors.shape)
            #raw_input()
            
            rh = {'image_vectors':image_vectors.tolist()}
            
            rh['_id'] = rec['_id']
            
            assert TASK_ID in ['order_model', 'order_model_2', 'order_model_3']
            
            rh['task_id'] = TASK_ID
            
            rr.append(rh)
        
        tot_t = time() - start_t
        
        per_sec = float(expected_batch_size) / tot_t
        
        assert expected_batch_size == len(rr), (expected_batch_size, len(rr))
        
        print ('OUTPUTTING',
               'expected_batch_size', expected_batch_size,
               'actual_size', len(rr),
               'time:', tot_t,
               'per_sec:', per_sec,
               )
        
        batch['batch'] = rr
        
        #print ('BATCH',batch)
        rr = json.dumps(batch)
        qq_output.put(rr)

        print ('UNLINKING_TEMP...')
        try:
            for xfn in fns:
                unlink(xfn)
        except:
            pass
        print ('UNLINKED_TEMP',)
