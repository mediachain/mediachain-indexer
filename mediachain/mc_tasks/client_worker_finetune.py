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


def client_worker_finetune(qq_input,
                           qq_output,
                           qq_shutdown,
                           TASK_ID,
                           ):
    """
    - generate vgg19s if they don't already exist.
    - run keras model
    """
    
    #assert False, 'WIP'
    
    assert TASK_ID in ['aesthetics_2', 'aesthetics_3'], repr(TASK_ID)
    
    if TASK_ID == 'aesthetics_2':
        model_name = 'model_vgg19_unsplash_v1' ## 512, dropout, 512
        """
        Epoch 20/20
        1s - loss: 0.0918 - acc: 0.9654 - val_loss: 0.8459 - val_acc: 0.7960
        accuracy: 0.796
        rows are predicted classes, columns are actual classes
        predict_neg     501     45
        predict_pos     159     295
        """
    elif TASK_ID == 'aesthetics_3':
        model_name = 'model_vgg19_500px_v1'    ## 512, dropout, 512
        """
        Epoch 20/20
        1s - loss: 0.1559 - acc: 0.9352 - val_loss: 0.8621 - val_acc: 0.7420
        accuracy: 0.742
        rows are predicted classes, columns are actual classes
        predict_neg     364     130
        predict_pos     128     378
        """
    else:
        assert False, TASK_ID
    
    field_name = VALID_TASKS[TASK_ID]['field_name']
    
    import json
    import numpy
    import os
    import numpy as np
    from os.path import split, join
    
    #os.environ['THEANO_FLAGS'] = 'mode=FAST_RUN,device=gpu,floatX=float32,device=gpu1'
    os.environ['KERAS_BACKEND'] = 'theano'

    import keras_finetuning
    from keras_finetuning import net

    model_base_dir = split(keras_finetuning.__file__)[0]
    
    extract_10crop = setup_extract_10crop()

    fn = join(model_base_dir, model_name)

    print ('LOADING MODEL', fn)
    
    model, tags_from_model = net.load(fn)
    #assert tags == tags_from_model
    net.compile(model)

    while True:
        try:
            batch = qq_input.get(timeout = 1)
        except:
            print ('CLIENT_WORKER_ORDER_MODEL_GET_QUEUE_TIMEOUT',current_thread().name)
            continue

        print ('GOT_BATCH', batch['batch_id'], len(batch))

        expected_batch_size = len(batch['batch'])

        start_t = time()
        tm_load = 0.000000000000001

        vv = []
        all_batch = {}
        found_native_ids = []
        for rec in batch['batch']:
            
            assert '_' in rec['_id'], repr(rec['_id'])
            hsh = hashlib.md5(str(rec['_id'])).hexdigest()
            
            all_batch[hsh] = rec

            t2 = time()
            
            fn2 = '/datasets/datasets/vgg19/' + hsh[:3] + '/'  + hsh + '.json'
            
            print ('LOAD', fn2)
            
            try:
                yy = np.load(fn2, mmap_mode='r')
            except:
                print ('BAD_FILE_OR_NOT_FOUND', fn2)
                tm_load += (time() - t2)
                continue
            
            tm_load += (time() - t2)
            
            found_native_ids.append(rec['_id'])
            vv.append(yy[0])

            if False:
                preds = model.predict(np.array([yy[0]]), batch_size=1)
                print ('predict_proba_once', hsh, preds)

        print ('LOAD_TIME', tm_load,'sec for', len(vv), '=', len(vv) / tm_load, 'per_sec')

        if not len(vv):
            print ('EMPTY_NO_FILES_FOUND')
            continue
        
        print ('PREDICTING_FOR', len(vv))
        
        t1 = time()
        preds = model.predict(np.array(vv), batch_size=len(vv))

        tm = time() - t1
        
        print ('PREDICT_TIME', tm, '=', len(vv) / tm, 'per_sec')
        
        #print ('predict_proba', preds)

        preds = preds[:,1] ## positive class
        
        rr = []
        for native_id, pred in zip(found_native_ids, preds):
            rh = {'_id':native_id}
            rh['score'] = float(pred) ## convert from numpy float, for the json serializer.
            rh['task_id'] = TASK_ID
            rh['created'] = int(t1)
            rr.append(rh)
        
        tot_t = time() - start_t
        
        per_sec = float(expected_batch_size) / tot_t
        
        #assert expected_batch_size == len(rr), (expected_batch_size, len(rr)) ## REMOVED for this task...
        
        print ('CLIENT_RESULTS_SAMPLE', rr[:5])
        
        #raw_input_enter()
        
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
