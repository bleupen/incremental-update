'use strict';

const _ = require('lodash');

exports.register = function (server, opts, next) {
    const qlpm = server.dm('queryLanguagePreProcessor')
    const { QueryLanguagePreprocessor, IneligibleQueryProcessorViolation } = qlpm;

    class AggPreProcessor extends QueryLanguagePreprocessor {
        constructor() {
            super();
            this.id = 'agg-preprocessor'
        }

        isEligible() {
            return true;
        }

        parseAggExpr(expr = '') {
            const [ fn, field ] = expr.split(':');
            if (fn === 'max') return { max: { field }};
            throw new Error('invalid expressions');
        }

        async search(ds, aggs) {
            try {
                const { aggregations } = await ds.search({ size: 0, body: { aggs } });
                return aggregations;
            } catch (err) {
                return {};
            }
        }

        async process(q, c) {
            try {
                const r = server.activeRequest();
                const ds = _.get(r, 'pre.dataset');
                if (!q.params || !Object.keys(q.params).length || !ds) return;

                const aggs = _.reduce(q.params, (aggs, value, key) => {
                    try {
                        return _.set(aggs, key, this.parseAggExpr(_([]).concat(value).first()));
                    } catch (err) {
                        return aggs;
                    }
                }, {});

                if (!Object.keys(aggs).length) return;

                const aggregations = await this.search(ds, aggs);
                const params = _.mapValues(aggs, (value, key) => _.get(aggregations, [ key, 'value_as_string' ], _.get(aggregations, [ key, 'value ' ])));
                q.params = params;
            } catch (err) {
                q.params
                console.error(err);
                // do nothing
            }
        }
    }

    qlpm.global(new AggPreProcessor());

    next();
};

exports.register.attributes = { name: 'incremental-update-plugin' };