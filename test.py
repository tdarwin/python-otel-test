from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    ConsoleSpanExporter,
)
from random import choice
from unfurler.unfurler import unfurl
from urllib.parse import urlparse


def get_urls(path):
    url_file = open(path, 'r')
    urls = url_file.read().strip().split('\n')
    url_file.close()
    span = trace.get_current_span()
    span.set_attribute("urls.original", len(urls))
    return urls


def get_used_urls(path):
    used_file = open(path, 'r')
    used = used_file.read().strip().split("\n")
    used_file.close()
    span = trace.get_current_span()
    span.set_attribute("urls.used", len(used))
    return used


def save_used_url(path, url):
    with open(path, 'a') as uf:
        uf.write(f'{url}\n')


def compare_lists(list1, list2):
    list3 = [x for x in list1 if x not in list2]
    span = trace.get_current_span()
    span.set_attribute("urls.remaining", len(list3))
    print('Remaining urls: ' + str(len(list3)))
    return len(list3)


def new_url(used_list, url_list):
    url = choice(url_list)
    if url in used_list:
        return None
    else:
        span = trace.get_current_span()
        span.set_attribute("url", url)
        return url


def main():
    with tracer.start_as_current_span("main-unfurling"):

        with tracer.start_as_current_span('fetch-urls-from-file'):
            urls = get_urls(url_file)
            used = get_used_urls(used_file)

        with tracer.start_as_current_span('sort-url-lists'):
            urls.sort()
            used.sort()

        with tracer.start_as_current_span('validate-urls-availability'):
            if urls == used or compare_lists(urls, used) == 0:
                print("all URLs have been unfurled")
                exit(2)

        with tracer.start_as_current_span('select-random-url'):
            url = new_url(used, urls)
            while url is None:
                url = new_url(used, urls)

            domain = urlparse(url).netloc
            span = trace.get_current_span()
            span.set_attribute("domain", domain)
            print(url)

        with tracer.start_as_current_span('unfurl-url'):
            unfurl_data = unfurl(url)
            span = trace.get_current_span()
            span.set_attribute("unfurl.all", str(unfurl_data))
            for key in unfurl_data:
                span.set_attribute("unfurl." + key, str(unfurl_data[key]))

            with tracer.start_as_current_span('save-url-as-used'):
                save_used_url(used_file, url)

    print(unfurl_data)


if __name__ == "__main__":
    provider = TracerProvider()
    processor = BatchSpanProcessor(OTLPSpanExporter(endpoint="https://api.honeycomb.io/"))
    provider.add_span_processor(processor)
    trace.set_tracer_provider(provider)

    tracer = trace.get_tracer('test')

    url_file = "./urls.txt"
    used_file = "./used.txt"

    main()
    provider.shutdown()
    # provider.force_flush()
