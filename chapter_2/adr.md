### Non-Functional Requirements (SLOs)

| SLO              | Target    |
|------------------|-----------|
| Latency (p99)    | < 200 ms  |
| Error rate       | < 0.1%    |
| Availability     | > 99.9%   |

### Capacity Plan
- Based on our measurements, we see that our p99 for latency breaches SLO at around 215 request/sec.
- Error rate and availabiliy is breached the moment we have one failure at the db disk or at the availabilities service respectively.
- For our scalability plan, the goal is to handle 1000 requests total, and to have minimum throughput of 215 req/s, and have our p99 latency < 200ms. Since we we reach max of latency SLO at around 215 req/sec, I recommend having at least 5 duplicate system (horizontal scaling) with a load balancer to distribute load to these for 5 availabilities system. This ensures that we are kept under the 200 latency threshold. We choose this over vertical scaling because we know that additional computation power on the same system scales worst than linearly with throughput.
- With the additional duplicated system, this also solves our problem of availability - if one of our availabilities system fail and need to restart, there is always another system up and running, and the load balander can re-distribute the work to those systems that are up.
- To solve our problem of errors, we would want some form of duplicated disk which keep the data of the whole system up to date. This is outside the scope of this chapter, but a general idea we could have is having one system be the source of truth, and whenever it gets a requests which modify the availabilities table, it forwards this change to all the other systems with request priority at the software level. When a another system receives an update request, it fotwards this request to the main serivce as a priority request. This ensures that the availabilities data is always duplicated.
- We note howerver, that it is a good idea to have more than 5 duplicated system, this is because we would want to keep the the ~215 req/s even if a minitory of systems fail to meet latency SLO requirement. Moreover, we need even more systems to acount for the disk updates between systems as these are also requests.

