#!/usr/bin/env python

def client_worker_tf(qq_input,
                     qq_output,
                     qq_shutdown,
                     TASK_ID,
                     modelFullPath = '/zdrive/unsplash_2/v1_output_graph.pb',
                     labelsFullPath = '/zdrive/unsplash_2/v1_output_labels.txt',
                     actual_labels = {'pos':'like_unsplash',
                                      'neg':'like_flickr',
                                      },
                     ):

    batch = {'batch':[]}

    ######
    
    import numpy as np
    import tensorflow as tf
    
    def create_graph():
        """Creates a graph from saved GraphDef file and returns a saver."""
        # Creates graph from saved graph_def.pb.
        with tf.gfile.FastGFile(modelFullPath, 'rb') as f:
            graph_def = tf.GraphDef()
            graph_def.ParseFromString(f.read())
            _ = tf.import_graph_def(graph_def, name='')

    # Creates graph from saved GraphDef.
    create_graph()

    with tf.Session() as sess:
        
        def run_inference_on_image(image_data):
            print ('run_inference_on_image()')
        

            answer = None

            softmax_tensor = sess.graph.get_tensor_by_name('final_result:0')
            predictions = sess.run(softmax_tensor,
                                   {'DecodeJpeg/contents:0': image_data})
            predictions = np.squeeze(predictions)

            top_k = predictions.argsort()[-5:][::-1]  # Getting top 5 predictions
            f = open(labelsFullPath, 'rb')
            lines = f.readlines()
            labels = [str(w).replace("\n", "") for w in lines]

            rh = {}

            for node_id in top_k:
                human_string = labels[node_id]
                score = predictions[node_id]
                #print('%s (score = %.5f)' % (human_string, score))

                out = actual_labels[human_string]
                
                #if human_string == 'pos':
                #    out = 'like_unsplash'
                #elif human_string == 'neg':
                #    out = 'like_flickr'
                #else:
                #    assert False, repr(human_string)

                rh[out] = float(score)

            answer = labels[top_k[0]]

            #return answer

            print (rh)

            return rh


        while True:
            try:
                batch = qq_input.get(timeout = 1)
            except:
                print ('CLIENT_WORKER_UNSPLASH_GET_QUEUE_TIMEOUT',current_thread().name)
                continue

            print ('GOT_BATCH', batch['batch_id'], len(batch))

            expected_batch_size = len(batch['batch'])

            start_t = time()
            rr = []
            for rec in batch['batch']:
                
                rh = run_inference_on_image(rec['data'])

                rh['_id'] = rec['_id']
                
                #assert TASK_ID.startswith('aes_unsplash_out')
                rh['task_id'] = TASK_ID

                rr.append(rh)
                
            tot_t = time() - start_t

            per_sec = float(expected_batch_size) / tot_t

            print ('OUTPUTTING',
                   'expected_batch_size',expected_batch_size,
                   'actual_size',len(rr),
                   'time:', tot_t,
                   'per_sec:', per_sec,
                   )

            batch['batch'] = rr

            #print ('BATCH',batch)
            rr = json.dumps(batch)
            qq_output.put(rr)
