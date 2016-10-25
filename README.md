
[<img src="https://github.com/mediachain/mediachain-indexer/raw/master/images_gh/diagram_2.png">](https://github.com/mediachain/mediachain-indexer/raw/master/images_gh/diagram_2.png)

## Search & Attribution for Mediachain

- [Install](https://github.com/mediachain/mediachain-indexer/blob/master/INSTALL.md)
- [API Docs](http://mediachainlabs-api-docs.s3-website-us-east-1.amazonaws.com/)
- [Live Frontend Demo](http://images.mediachain.io)


### Achieving high-aesthetics image search

Mediachain Indexer Core uses state of the art ML to provide high-quality image search on the world's 400 million+ Creative Commons licensed images. This engine unites image datasets from over 30 of the most important CC sources, and ranks results both aesthetically and in terms of query relevance.


### Aesthetics quality

Mediachain neural aesthetics model predictions for high (top) and low (bottom) image aesthetics:
![](https://github.com/mediachain/mediachain-indexer/raw/master/images_gh/160819-0007.png)
![](https://github.com/mediachain/mediachain-indexer/raw/master/images_gh/160819-0008.png)

Achieving a human-like sense of aesthetics in a computer model is a major challenge. The Mediachain Search API achieves this using the latest end-to-end supervised trained deep neural networks. These aesthetics models are key to maintaining high search results quality while taking advantage of the huge un-curated Creative Commons resources, which have a lower typical image quality than much smaller curated collections.

![](https://github.com/mediachain/mediachain-indexer/raw/master/images_gh/160819-0002.png)
![](https://github.com/mediachain/mediachain-indexer/raw/master/images_gh/160819-0001.png)
![](https://github.com/mediachain/mediachain-indexer/raw/master/images_gh/160819-0003.png)


### Query Relevance

Search relevance between queries and images is the second major component of the Mediachain image search. The query relevance models are state of the art models trained to learn multi-modality visual and textual representations of semantic similarity. These models are able to learn the visual meaning of images directly from their raw pixels.


### Balancing aesthetics and relevance using supervised re-ranking

The third major consideration of a high aesthetics image search is achieving the optimal blend of relevance to the query, and a general sense of image aesthetics quality. The degree to which each of these factors affect the final ranking not only depends on the general behavior of the models, but is also highly query-dependent.

Combining the many notions of results quality into a single ranking often leads to conflicts. E.g. the highest quality image may not be the most relevant to the query. In order to resolve these conflicting notions of quality, while optimizing results, the Mediachain API employs a sophisticated reranking model.

