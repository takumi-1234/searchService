import grpc from 'k6/net/grpc';
import { check, sleep } from 'k6';

const client = new grpc.Client();
client.load(
  ['../../../tests/perf/proto', '../../../api/proto'],
  'search/v1/search.proto',
);

const vus = Number(__ENV.K6_VUS || 10);
const duration = __ENV.K6_DURATION || '30s';
const target = __ENV.GRPC_TARGET || '127.0.0.1:50071';

export const options = {
  vus,
  duration,
  thresholds: {
    grpc_req_duration: ['p(95)<500'],
    checks: ['rate>0.99'],
  },
};

export function setup() {
  return { target };
}

export default function (data) {
  client.connect(data.target, { plaintext: true });

  const response = client.invoke('api.proto.search.v1.SearchService/SearchDocuments', {
    index_name: 'books',
    query_text: 'calculus',
    page_size: 2,
  });

  check(response, {
    'status is OK': (r) => r && r.status === grpc.StatusOK,
    'received results': (r) => r && r.message && r.message.results && r.message.results.length > 0,
  });

  client.close();
  sleep(1);
}
