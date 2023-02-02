const _ = require("lodash");

const memoizeOne = require("memoize-one");

const { cartesianProduct } = require("../../utils/SetUtils");

const WEEKDAY = "WEEKDAY";
const WEEKEND = "WEEKEND";
const NUM_MONTHS_IN_YEAR = 12;
const NUM_DAYS_IN_WEEK = 7;
const MINUTES_PER_EPOCH = 5;
const EPOCHS_PER_DAY = 288;

const {
  profiles: trafficDistributionProfiles
} = require("../trafficDistributionsController");

const trafficDistributions5minBin = _.mapValues(
  trafficDistributionProfiles,
  tdp =>
    _(tdp)
      .map(hrCt => _.fill(Array(12), hrCt / 12))
      .flatten()
      .value()
);

const monthAdjFactors = [
  0.94,
  0.88,
  1.01,
  1.01,
  1.05,
  1.04,
  1.05,
  1.08,
  0.99,
  1.04,
  0.95,
  0.97
];

const dowAdjFactors = [0.8, 1.05, 1.05, 1.05, 1.05, 1.1, 0.9];

const getNumBinsInDayForTimeBinSize = memoizeOne(timeBinSize =>
  Math.floor((5 / timeBinSize) * EPOCHS_PER_DAY)
);

const getTrafficDistributionProfileName = ({
  dayType,
  congestionLevel,
  directionality,
  functionalClass
}) =>
  dayType === WEEKDAY
    ? `${dayType}_${congestionLevel}_${directionality}_${functionalClass}`
    : `${dayType}_${functionalClass}`;

const getTimeBinnedTrafficDistributionProfile = memoizeOne(
  ({ trafficDistributionProfileName, timeBinSize }) => {
    const tdp5min = trafficDistributions5minBin[trafficDistributionProfileName];

    const timeBinnedTrafficDistributionProfile = _(tdp5min)
      .chunk(timeBinSize / MINUTES_PER_EPOCH)
      .map(_.sum)
      .value();

    return timeBinnedTrafficDistributionProfile;
  },
  _.isEqual
);

// returns a 3-d array.
//   1st dimension has length = 12 (months, zero indexed)
//   2nd dimension has length = 7 (dows)
//   3rd dimension has length = num timeBins in day for timeBinSize (values = fraction of daily aadt)
const getFractionOfDailyAadtByMonthByDowByTimeBin = memoizeOne(
  ({ functionalClass, congestionLevel, directionality, timeBinSize }) => {
    // Traffic Distribution Profiles at timeBinSize resolution
    const profiles = [WEEKEND, WEEKDAY].reduce((acc, dayType) => {
      const trafficDistributionProfileName = getTrafficDistributionProfileName({
        dayType,
        congestionLevel,
        directionality,
        functionalClass
      });

      acc[dayType] = getTimeBinnedTrafficDistributionProfile({
        trafficDistributionProfileName,
        timeBinSize
      });

      return acc;
    }, {});

    const numBinsInDay = getNumBinsInDayForTimeBinSize(timeBinSize);

    // Fraction of dailyAadt (aadt / 365) throughout the week's timebins
    const fractionOfDailyAadtByDowByTimeBin = cartesianProduct(
      _.range(NUM_MONTHS_IN_YEAR),
      _.range(NUM_DAYS_IN_WEEK),
      _.range(numBinsInDay)
    ).reduce((acc, [month, dow, timeBinNum]) => {
      const monthAdjustmentFactor = monthAdjFactors[month];
      const dowAdjustmentFactor = dowAdjFactors[dow];

      const trafficDistributionProfile = profiles[dow % 6 ? WEEKDAY : WEEKEND];

      const fractionOfDailyAadt = trafficDistributionProfile[timeBinNum];

      acc[month] = acc[month] || [];
      acc[month][dow] = acc[month][dow] || [];
      acc[month][dow][timeBinNum] =
        fractionOfDailyAadt * monthAdjustmentFactor * dowAdjustmentFactor;

      return acc;
    }, []);

    return fractionOfDailyAadtByDowByTimeBin;
  },
  _.isEqual
);

module.exports = {
  getFractionOfDailyAadtByMonthByDowByTimeBin
};
