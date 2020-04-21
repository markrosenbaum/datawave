package datawave.query.util;

import com.vividsolutions.jts.geom.Envelope;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.GeometryCollection;
import com.vividsolutions.jts.geom.GeometryFactory;
import datawave.data.normalizer.GeometryNormalizer;
import mil.nga.giat.geowave.core.index.ByteArrayId;
import mil.nga.giat.geowave.core.index.ByteArrayRange;
import mil.nga.giat.geowave.core.index.sfc.data.MultiDimensionalNumericData;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
 * This utility class contains a variety of methods perform operations on GeoWave ranges.
 *
 * These methods assume that a full incremental tiered index strategy is being used, with a maximum of 31 bits per dimension, and using the Hilbert
 * Space-Filling Curve. No guarantees are made as to the effectiveness or accuracy of these methods given any other configuration.
 */
public class GeoWaveUtils {
    
    /**
     * Optimizes the list of byte array ranges needed to query the desired area. Portions of each range which do not intersect the original query polygon will
     * be pruned out.
     * 
     * @param queryGeometry
     *            the original query geometry used to create the list of byte array ranges
     * @param byteArrayRanges
     *            the original byte array ranges generated for the query geometry
     * @param rangeSplitThreshold
     *            used to determine the minimum number of segments to break a range into - higher values will take longer to computer, but will yield tighter
     *            ranges
     * @param maxRangeOverlap
     *            the maximum amount of overlap a range is allowed to have compared to the envelope of the query geometry - expressed as a double between 0 and
     *            1.
     * @return a list of optimized byte array ranges
     */
    public static List<ByteArrayRange> optimizeByteArrayRanges(Geometry queryGeometry, List<ByteArrayRange> byteArrayRanges, int rangeSplitThreshold,
                    double maxRangeOverlap) {
        List<ByteArrayRange> optimizedRanges = new ArrayList<>();
        for (ByteArrayRange byteArrayRange : byteArrayRanges) {
            if (!byteArrayRange.isSingleValue()) {
                optimizedRanges.addAll(optimizeByteArrayRange(queryGeometry, byteArrayRange, rangeSplitThreshold, maxRangeOverlap));
            } else {
                optimizedRanges.add(byteArrayRange);
            }
        }
        return optimizedRanges;
    }
    
    /**
     * Optimizes the list of byte array ranges needed to query the desired area. Portions of each range which do not intersect the original query polygon will
     * be pruned out.
     * 
     * @param queryGeometry
     *            the original query geometry used to create the list of byte array ranges
     * @param byteArrayRange
     *            a byte array range representing a portion of the query geometry
     * @param rangeSplitThreshold
     *            used to determine the minimum number of segments to break a range into - higher values will take longer to computer, but will yield tighter
     *            ranges
     * @param maxRangeOverlap
     *            the maximum amount of overlap a range is allowed to have compared to the envelope of the query geometry - expressed as a double between 0 and
     *            1.
     * @return a list of optimized byte array ranges
     */
    public static List<ByteArrayRange> optimizeByteArrayRange(Geometry queryGeometry, ByteArrayRange byteArrayRange, int rangeSplitThreshold,
                    double maxRangeOverlap) {
        GeometryFactory gf = new GeometryFactory();
        List<ByteArrayRange> byteArrayRanges = new ArrayList<>();
        
        int tier = decodeTier(byteArrayRange);
        if (tier == 0) {
            byteArrayRanges.add(byteArrayRange);
        } else {
            ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
            
            long min = decodePosition(byteArrayRange.getStart(), longBuffer);
            long max = decodePosition(byteArrayRange.getEnd(), longBuffer);
            long range = max - min + 1;
            
            // It's too expensive to check every geohash in the range to see if it
            // intersects with the original query geometry, so we will attempt to project
            // this range to an equivalent range at a lower granularity tier to minimize
            // the number of geohashes we need to check. By doing this, we can adjust
            // the level of granularity used to prune our ranges.
            // This is controlled by modifying the chunks per range. Higher chunks per
            // range will achieve greater pruning, but will be more expensive to compute,
            // and will introduce more query ranges (which has performance implications
            // as well).
            for (int curTier = 0; curTier <= tier; curTier++) {
                long scale = (long) Math.pow(2.0, 2.0 * (tier - curTier));
                
                if (range >= scale) {
                    long scaledMin = (long) Math.ceil((double) min / scale);
                    long scaledMax = max / scale;
                    
                    if ((scaledMax - scaledMin + 1) >= rangeSplitThreshold) {
                        boolean simplifiedRanges = false;
                        long subRangeMin = scaledMin * scale;
                        long subRangeMax = Long.MIN_VALUE;
                        
                        for (long scaledPos = scaledMin; scaledPos <= scaledMax; scaledPos++) {
                            long nextSubRangeMax = (scaledPos * scale + scale - 1);
                            
                            if (nextSubRangeMax <= max) {
                                simplifiedRanges = true;
                                subRangeMax = nextSubRangeMax;
                                
                                // make sure that this condensed hash is within the bounds of the map
                                ByteArrayId scaledId = createByteArrayId(curTier, scaledPos, longBuffer);
                                MultiDimensionalNumericData scaledBounds = GeometryNormalizer.indexStrategy.getRangeForId(scaledId);
                                
                                // make sure that the scaled id is within the bounds of the map
                                // note: all cells for tiers 0 and 1 are within the bounds of the map
                                if (inBounds(scaledBounds) || curTier <= 1) {
                                    // @formatter:off
                                    Geometry scaledGeom = gf.toGeometry(
                                            new Envelope(
                                                    scaledBounds.getMinValuesPerDimension()[0],
                                                    scaledBounds.getMaxValuesPerDimension()[0],
                                                    scaledBounds.getMinValuesPerDimension()[1],
                                                    scaledBounds.getMaxValuesPerDimension()[1]));
                                    // @formatter:on
                                    
                                    // make sure that the scaled geometry intersects the original query geometry
                                    if (scaledGeom.intersects(queryGeometry)) {
                                        byteArrayRanges.add(createByteArrayRange(tier, scaledPos * scale, scaledPos * scale + scale - 1, longBuffer));
                                    }
                                }
                            } else {
                                break;
                            }
                        }
                        
                        if (simplifiedRanges) {
                            if (min < subRangeMin && rangeToGeometry(tier, min, subRangeMin - 1).intersects(queryGeometry)) {
                                byteArrayRanges.add(createByteArrayRange(tier, min, subRangeMin - 1, longBuffer));
                            }
                            
                            if (max > subRangeMax && rangeToGeometry(tier, subRangeMax + 1, max).intersects(queryGeometry)) {
                                byteArrayRanges.add(createByteArrayRange(tier, subRangeMax + 1, max, longBuffer));
                            }
                            break;
                        }
                    }
                }
            }
            
            if (byteArrayRanges.isEmpty() && rangeToGeometry(tier, min, max).intersects(queryGeometry)) {
                byteArrayRanges.add(byteArrayRange);
            } else {
                byteArrayRanges = mergeContiguousRanges(byteArrayRanges, longBuffer);
                byteArrayRanges = splitLargeRanges(byteArrayRanges, queryGeometry, maxRangeOverlap, longBuffer);
            }
        }
        
        return byteArrayRanges;
    }
    
    /**
     * Merges contiguous ranges in the list - assumes that the list of ranges is already sorted
     * 
     * @param byteArrayRanges
     *            the sorted list of ranges to merge
     * @return a list of merged ranges
     */
    public static List<ByteArrayRange> mergeContiguousRanges(List<ByteArrayRange> byteArrayRanges) {
        return mergeContiguousRanges(byteArrayRanges, null);
    }
    
    /**
     * Merges contiguous ranges in the list - assumes that the list of ranges is already sorted
     * 
     * @param byteArrayRanges
     *            the sorted list of ranges to merge
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a list of merged ranges
     */
    public static List<ByteArrayRange> mergeContiguousRanges(List<ByteArrayRange> byteArrayRanges, ByteBuffer longBuffer) {
        longBuffer = (longBuffer != null && longBuffer.array().length == Long.BYTES) ? longBuffer : ByteBuffer.allocate(Long.BYTES);
        List<ByteArrayRange> mergedByteArrayRanges = new ArrayList<>(byteArrayRanges.size());
        ByteArrayRange currentRange = null;
        
        for (ByteArrayRange range : byteArrayRanges) {
            if (currentRange == null) {
                currentRange = range;
            } else {
                long currentMax = decodePosition(currentRange.getEnd(), longBuffer);
                long nextMin = decodePosition(range.getStart(), longBuffer);
                
                if ((currentMax + 1) == nextMin) {
                    currentRange = new ByteArrayRange(currentRange.getStart(), range.getEnd(), false);
                } else {
                    mergedByteArrayRanges.add(currentRange);
                    currentRange = range;
                }
            }
        }
        
        if (currentRange != null) {
            mergedByteArrayRanges.add(currentRange);
        }
        
        return mergedByteArrayRanges;
    }
    
    /**
     * Splits ranges whose area overlaps more than maxRangeOverlap of the area of the queryGeometry envelope.
     * 
     * @param byteArrayRanges
     *            the list of ranges to split
     * @param queryGeometry
     *            the original query geometry
     * @param maxRangeOverlap
     *            the maximum percentage overlap allowed for a range compared to the envelope of the original query geometry
     * @return a list of ranges, each of which overlaps less than maxRangeOverlap of the original query geometry
     */
    public static List<ByteArrayRange> splitLargeRanges(List<ByteArrayRange> byteArrayRanges, Geometry queryGeometry, double maxRangeOverlap) {
        return splitLargeRanges(byteArrayRanges, queryGeometry, maxRangeOverlap, null);
    }
    
    /**
     * Splits ranges whose area overlaps more than maxRangeOverlap of the area of the queryGeometry envelope.
     * 
     * @param byteArrayRanges
     *            the list of ranges to split
     * @param queryGeometry
     *            the original query geometry
     * @param maxRangeOverlap
     *            the maximum percentage overlap allowed for a range compared to the envelope of the original query geometry
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return a list of ranges, each of which overlaps less than maxRangeOverlap of the original query geometry
     */
    public static List<ByteArrayRange> splitLargeRanges(List<ByteArrayRange> byteArrayRanges, Geometry queryGeometry, double maxRangeOverlap,
                    ByteBuffer longBuffer) {
        longBuffer = (longBuffer != null && longBuffer.array().length == Long.BYTES) ? longBuffer : ByteBuffer.allocate(Long.BYTES);
        List<ByteArrayRange> splitByteArrayRanges = new ArrayList<>();
        
        for (ByteArrayRange range : byteArrayRanges) {
            int tier = decodeTier(range.getStart());
            long min = decodePosition(range.getStart(), longBuffer);
            long max = decodePosition(range.getEnd(), longBuffer);
            
            Geometry rangeGeometry = rangeToGeometry(tier, min, max);
            if (rangeGeometry.getArea() > maxRangeOverlap * queryGeometry.getEnvelope().getArea()) {
                int numSubRanges = (int) (rangeGeometry.getArea() / (maxRangeOverlap * queryGeometry.getEnvelope().getArea())) + 1;
                long offset = (max - min) / numSubRanges;
                
                for (int i = 0; i < numSubRanges; i++) {
                    long subMax = ((i + 1) == numSubRanges) ? max : min + (i + 1) * offset - 1;
                    splitByteArrayRanges.add(createByteArrayRange(tier, min + i * offset, subMax, longBuffer));
                }
            } else {
                splitByteArrayRanges.add(range);
            }
        }
        return splitByteArrayRanges;
    }
    
    /**
     * Extracts the tier from the byteArrayRange
     * 
     * @param byteArrayRange
     * @return
     */
    public static int decodeTier(ByteArrayRange byteArrayRange) {
        return decodeTier(byteArrayRange.getStart());
    }
    
    /**
     * Extracts the tier from the byteArrayId
     * 
     * @param byteArrayId
     * @return
     */
    public static int decodeTier(ByteArrayId byteArrayId) {
        return byteArrayId.getBytes()[0];
    }
    
    /**
     * Extracts the position from the byteArrayId
     * 
     * @param byteArrayId
     * @return
     */
    public static long decodePosition(ByteArrayId byteArrayId) {
        return decodePosition(byteArrayId, null);
    }
    
    /**
     * Extracts the position from the byteArrayId
     * 
     * @param byteArrayId
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return
     */
    public static long decodePosition(ByteArrayId byteArrayId, ByteBuffer longBuffer) {
        longBuffer = (longBuffer != null && longBuffer.array().length == Long.BYTES) ? longBuffer : ByteBuffer.allocate(Long.BYTES);
        longBuffer.clear();
        
        longBuffer.position(Long.BYTES - (byteArrayId.getBytes().length - 1));
        longBuffer.put(byteArrayId.getBytes(), 1, byteArrayId.getBytes().length - 1);
        return longBuffer.getLong(0);
    }
    
    /**
     * Determines the number of hex characters needed to represent a position at a given tier. This excludes the byte reserved for the tier identifier.
     * 
     * @param tier
     * @return
     */
    public static int hexCharsPerTier(int tier) {
        String hexString = String.format("%X", ((long) Math.pow(2.0, tier) - 1));
        if (Long.parseLong(hexString, 16) == 0)
            return 0;
        else
            return hexString.length() * 2;
    }
    
    /**
     * Creates a ByteArrayId from the given tier and position
     * 
     * @param tier
     * @param position
     * @return
     */
    public static ByteArrayId createByteArrayId(int tier, long position) {
        return createByteArrayId(tier, position, null);
    }
    
    /**
     * Creates a ByteArrayId from the given tier and position
     * 
     * @param tier
     * @param position
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return
     */
    public static ByteArrayId createByteArrayId(int tier, long position, ByteBuffer longBuffer) {
        longBuffer = (longBuffer != null && longBuffer.array().length == Long.BYTES) ? longBuffer : ByteBuffer.allocate(Long.BYTES);
        longBuffer.clear();
        
        ByteBuffer buffer = ByteBuffer.allocate(hexCharsPerTier(tier) / 2 + 1);
        buffer.put((byte) tier);
        longBuffer.putLong(position);
        buffer.put(longBuffer.array(), longBuffer.capacity() - buffer.remaining(), buffer.remaining());
        
        return new ByteArrayId(buffer.array());
    }
    
    /**
     * Creates a ByteArrayRange from the given tier, and min & max positions
     * 
     * @param tier
     * @param min
     * @param max
     * @return
     */
    public static ByteArrayRange createByteArrayRange(int tier, long min, long max) {
        return createByteArrayRange(tier, min, max, null);
    }
    
    /**
     * Creates a ByteArrayRange from the given tier, and min & max positions
     * 
     * @param tier
     * @param min
     * @param max
     * @param longBuffer
     *            a reusable byte buffer of Long.BYTES length
     * @return
     */
    public static ByteArrayRange createByteArrayRange(int tier, long min, long max, ByteBuffer longBuffer) {
        longBuffer = (longBuffer != null && longBuffer.array().length == Long.BYTES) ? longBuffer : ByteBuffer.allocate(Long.BYTES);
        
        return new ByteArrayRange(createByteArrayId(tier, min, longBuffer), createByteArrayId(tier, max, longBuffer), min == max);
    }
    
    /**
     * Determines whether the given bounds are within the bounds of the map.
     * 
     * @param bounds
     * @return
     */
    public static boolean inBounds(MultiDimensionalNumericData bounds) {
        return bounds.getMinValuesPerDimension()[0] >= -180 && bounds.getMinValuesPerDimension()[0] <= 180 && bounds.getMaxValuesPerDimension()[0] >= -180
                        && bounds.getMaxValuesPerDimension()[0] <= 180 && bounds.getMinValuesPerDimension()[1] >= -90
                        && bounds.getMinValuesPerDimension()[1] <= 90 && bounds.getMaxValuesPerDimension()[1] >= -90
                        && bounds.getMaxValuesPerDimension()[1] <= 90;
    }
    
    /**
     * Given a range at a given tier, this will generate a Geometry which represents that range.
     * 
     * @param tier
     * @param min
     * @param max
     * @return
     */
    public static Geometry rangeToGeometry(int tier, long min, long max) {
        GeometryFactory gf = new GeometryFactory();
        
        List<ByteArrayId> byteArrayIds = decomposeRange(tier, min, max);
        
        List<Geometry> geometries = new ArrayList<>(byteArrayIds.size());
        for (ByteArrayId byteArrayId : byteArrayIds) {
            MultiDimensionalNumericData bounds = GeometryNormalizer.indexStrategy.getRangeForId(byteArrayId);
            
            if ((inBounds(bounds) || tier <= 1)) {
                // @formatter:off
                geometries.add(gf.toGeometry(
                        new Envelope(
                                bounds.getMinValuesPerDimension()[0],
                                bounds.getMaxValuesPerDimension()[0],
                                bounds.getMinValuesPerDimension()[1],
                                bounds.getMaxValuesPerDimension()[1])));
                // @formatter:on
            }
        }
        
        return new GeometryCollection(geometries.toArray(new Geometry[0]), gf).union();
    }
    
    /**
     * This is a convenience class used within decomposeRange.
     */
    private static class TierMinMax {
        public int tier;
        public long min;
        public long max;
        
        public TierMinMax(int tier, long min, long max) {
            this.tier = tier;
            this.min = min;
            this.max = max;
        }
    }
    
    /**
     * This performs a sort of quad-tree decomposition on the given range. This algorithm searched for subranges within the original range which can be
     * represented in a simplified fashion at a lower granularity tier. The resulting list of byteArrayRanges will consist of an equivalent set of ranges,
     * spread out across multiple tiers, which is topologically equivalent to the footprint of the original range.
     * 
     * @param tier
     * @param min
     * @param max
     * @return
     */
    public static List<ByteArrayId> decomposeRange(int tier, long min, long max) {
        List<ByteArrayId> byteArrayIds = new ArrayList<>();
        ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);
        
        LinkedList<TierMinMax> queue = new LinkedList<>();
        queue.push(new TierMinMax(0, min, max));
        
        while (!queue.isEmpty()) {
            TierMinMax tierMinMax = queue.pop();
            long range = tierMinMax.max - tierMinMax.min + 1;
            
            while (tierMinMax.tier <= tier) {
                long scale = (long) Math.pow(2.0, 2.0 * (tier - tierMinMax.tier));
                
                if (range >= scale) {
                    long scaledMin = (long) Math.ceil((double) min / scale);
                    long scaledMax = max / scale;
                    
                    boolean simplifiedRanges = false;
                    long subRangeMin = scaledMin * scale;
                    long subRangeMax = Long.MIN_VALUE;
                    
                    for (long scaledPos = scaledMin; scaledPos <= scaledMax; scaledPos++) {
                        long nextSubRangeMax = (scaledPos * scale + scale - 1);
                        
                        if (nextSubRangeMax <= max) {
                            simplifiedRanges = true;
                            subRangeMax = nextSubRangeMax;
                            
                            byteArrayIds.add(createByteArrayId(tierMinMax.tier, scaledPos, longBuffer));
                        } else {
                            break;
                        }
                    }
                    
                    if (simplifiedRanges) {
                        if (tierMinMax.min < subRangeMin) {
                            queue.push(new TierMinMax(tierMinMax.tier + 1, tierMinMax.min, subRangeMin - 1));
                        }
                        
                        if (subRangeMax < tierMinMax.max) {
                            queue.push(new TierMinMax(tierMinMax.tier + 1, subRangeMax + 1, tierMinMax.max));
                        }
                        
                        break;
                    }
                }
                
                tierMinMax.tier++;
            }
        }
        return byteArrayIds;
    }
}
