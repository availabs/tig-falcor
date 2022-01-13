const _ = require("lodash");

const db_service = require("../tig_db");

async function tigNPMRDS(tmcs, month, year) {
  try {
    const sql = `
      SELECT
          tmc,
          hr_0,
          hr_1,
          hr_2,
          hr_3,
          hr_4,
          hr_5,
          hr_6,
          hr_7,
          hr_8,
          hr_9,
          hr_10,
          hr_11,
          hr_12,
          hr_13,
          hr_14,
          hr_15,
          hr_16,
          hr_17,
          hr_18,
          hr_19,
          hr_20,
          hr_21,
          hr_22,
          hr_23
        FROM public.npmrds_monthly_avg_tt
          INNER JOIN (
            SELECT UNNEST($1::TEXT[])
          ) AS t(tmc) USING (tmc)
        WHERE (
          ( year = $2 )
          AND
          ( month = $3 )
        )
    `;

    const { rows } = await db_service.query(sql, [_.uniq(tmcs), year, month]);

    return rows;
  } catch (err) {
    console.error(err);
    throw err;
  }
}

module.exports = {
  tigNPMRDS
};
