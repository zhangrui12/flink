package flink;

import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;

public class TimeStamp implements AssignerWithPeriodicWatermarks<HotItems.UserBehavior> {
    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        return new Watermark(System.currentTimeMillis());
    }

    @Override
    public long extractTimestamp(HotItems.UserBehavior element, long previousElementTimestamp) {
        return element.timestamp * 1000;
    }
}
