import sys
from pprint import pprint
import time

class Endpoint():
    def __init__(self, i, latency_to_datacenter, number_of_caches, caches):
        self.id = i
        self.caches = caches
        self.number_of_caches = number_of_caches
        self.latency_to_datacenter = latency_to_datacenter
        self.fastest_caches = sorted(caches.values(), key=lambda c: c[1], reverse=True)

    def __repr__(self):
        return "{Endpoint[%s] caches[%s] lat[%s]}" % (self.id, self.caches, self.latency_to_datacenter)


class Request():
    def __init__(self, video_id, video_size, endpoint_id, number_of_requests):
        self.remaining_requests = number_of_requests
        self.endpoint_id = endpoint_id
        self.video_size = video_size
        self.video_id = video_id
        self.number_of_requests = number_of_requests

    def __repr__(self):
        return "{Request count[%d] video[%s] size[%s] endpoint[%s] remaining[%s]}" % (self.number_of_requests, self.video_id, self.video_size, self.endpoint_id, self.remaining_requests)


class Cache():
    def __init__(self, index, capacity):
        self.index = index
        self.capacity = capacity

    def __repr__(self):
        return "{Cache[%s] capacity[%s}" % (self.index, self.capacity)


class Video():
    def __init__(self, index, size):
        self.index = index
        self.size = size
        self.caches_served_from = set()


def list_str_to_int(l):
    return [int(x) for x in l]


def parse(filename):
    with open(filename, 'r') as f:
        v, e, r, c, x = list_str_to_int(f.readline().rstrip().split())
        videos = list_str_to_int(f.readline().rstrip().split())
        endpoints = []
        caches = []
        requests = []

        for i in range(0, c):
            caches.append(Cache(i, x))

        for i in range(0, e):
            latency_to_datacenter, number_of_caches = list_str_to_int(f.readline().rstrip().split())
            endpoint_caches = {}
            for j in range(0, number_of_caches):
                cache_id, latency_to_cache = list_str_to_int(f.readline().rstrip().split())
                endpoint_caches[cache_id]= (caches[cache_id], latency_to_cache)
            endpoints.append(Endpoint(i, latency_to_datacenter, number_of_caches, endpoint_caches))

        for i in range(0, r):
            video_id, endpoint_id, number_of_requests = list_str_to_int(f.readline().rstrip().split())
            video_size = videos[video_id]
            requests.append(Request(video_id, video_size, endpoint_id, number_of_requests))

        return videos, endpoints, caches, requests


def find_if_video_is_cached_from_endpoint(video_id, endpoint, results):
    for (cache, videos) in results.items():
        if video_id in videos:
            if cache.index in endpoint.caches:
                return cache
    return None


def compute_score(requests, endpoints, results):
    averages = 0
    total_requests = 0
    for r in requests:
        endpoint = endpoints[r.endpoint_id]
        lat_to_dc = endpoint.latency_to_datacenter
        cache = find_if_video_is_cached_from_endpoint(r.video_id, endpoint, results)
        # 0 if this endpoint's request is not served from a cache
        latency_saved = 0
        if cache is not None:
            latency_saved = lat_to_dc - endpoint.caches[cache.index][1]
        averages += latency_saved * r.number_of_requests
        total_requests += r.number_of_requests
    return averages / (1.0 * total_requests) * 1000


def build_clusters(endpoints, requests):
    clusters = {}
    for r in sorted(requests, key=lambda r: r.endpoint_id):
        if r.endpoint_id not in clusters:
            clusters[r.endpoint_id] = []
        clusters[r.endpoint_id].append(r)
    map(lambda c: clusters[c].sort(key=lambda r: r.number_of_requests, reverse=True), clusters.keys())
    return clusters


def can_still_serve(cache, video_size):
    return cache.capacity - video_size > 0


def get_the_next_best_cache_to_use_from_endpoint(endpoint, video_size):
    for cache in endpoint.fastest_caches:
        if can_still_serve(cache[0], video_size):
            return cache[0]
    return None


def get_next_elligible_request(clusters):
    c = sorted(clusters.items(), key=lambda r: sum(x.remaining_requests for x in r[1]), reverse=True)
    c[0][1].sort(key=lambda x: x.remaining_requests, reverse=True)
    return clusters[c[0][0]][0]


def elligible_requests_to_serve_still_available(clusters):
    return any([r.remaining_requests != 0 for c, x in clusters.items() for r in x])


def serve_clusters_to_caches(endpoints, clusters, caches):
    result_set = {}
    unserved = 0
    while elligible_requests_to_serve_still_available(clusters):
        # print clusters
        r = get_next_elligible_request(clusters)
        sys.stdout.write('.')
        # print clusters
        cache = get_the_next_best_cache_to_use_from_endpoint(endpoints[r.endpoint_id], r.video_size)
        if cache:
            if cache not in result_set:
                result_set[cache] = []
            # video already served
            if r.video_id in result_set[cache]:
                r.remaining_requests = 0
                continue
            result_set[cache].append(r.video_id)
            cache.capacity -= r.video_size
            # request has been cached !
            r.remaining_requests = 0
        else:
            unserved += 1
            # unservable...
            r.remaining_requests = 0
            continue
    print 'DONE'
    print unserved
    return result_set


def print_result_set(result_set, output):
    with open(output, 'w') as f:
        f.write(str(len(result_set)))
        f.write('\n')
        for (cache, videos) in result_set.items():
            f.write(str(cache.index) + ' ')
            f.write(" ".join([str(x) for x in videos]))
            f.write('\n')


def process(filename):
    print "Processing %s" % filename
    videos, endpoints, caches, requests = parse('../dataset/' + filename)
    clusters = build_clusters(endpoints, requests)
    result_set = serve_clusters_to_caches(endpoints, clusters, caches)
    # for (cache, videos) in result_set.items():
    #    print cache, videos
    print_result_set(result_set, '../output/' + filename.split('.')[0] + '.out')
    print compute_score(requests, endpoints, result_set)


def main():
    # for filename in ('me_at_the_zoo.in', 'trending_today.in', 'videos_worth_spreading.in', 'kittens.in'):
    for filename in ('me_at_the_zoo.in',):
        start = time.time()
        process(filename)
        end = time.time()
        print(end - start)


if __name__ == '__main__':
    main()