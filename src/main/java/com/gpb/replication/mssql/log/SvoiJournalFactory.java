package com.gpb.replication.mssql.log;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.boot.info.BuildProperties;
import org.springframework.context.event.EventListener;
import org.springframework.core.annotation.Order;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class SvoiJournalFactory {
    private final SimpleDateFormat DATE_FORMAT_PATTERN = new SimpleDateFormat("MMM dd yyyy HH:mm:ss");
    private String deviceProduct;
    private String deviceVersion;
    @Value("$server.port")
    private Integer localPort;
    private String localHostName;
    private Long journalLineNumber = 0L;

    @Autowired(required = false)
    private BuildProperties buildProperties;

    public SvoiJournal getJournalSource() {
        SvoiJournal svoiJournal = this.getBaseJournal();
        svoiJournal.setDvchost(this.getLocalHostName());
        svoiJournal.setStart(new Date());
        return svoiJournal;
    }

    public SvoiJournal getJournalTarget() {
        SvoiJournal svoiJournal = this.getBaseJournal();
        svoiJournal.setDvchost(this.getLocalHostName());
        svoiJournal.setEnd(new Date());
        return svoiJournal;
    }

    private SvoiJournal getBaseJournal() {
        Date now = new Date();
        SvoiJournal svoiJournal = new SvoiJournal(this.nextLineNumber(), this.DATE_FORMAT_PATTERN);
        svoiJournal.setDeviceProduct(this.buildProperties != null ? this.buildProperties.getName() : this.deviceProduct);
        svoiJournal.setDeviceVersion(this.buildProperties != null ? this.buildProperties.getVersion() : this.deviceVersion);
        svoiJournal.setTime(now);
        svoiJournal.setApp("TCP");
        svoiJournal.setRt(now);
        svoiJournal.setDeviceProcessName("java");
        return svoiJournal;
    }

    private synchronized Long nextLineNumber() {
        return this.journalLineNumber = this.journalLineNumber + 1L;
    }

    @EventListener({ApplicationStartedEvent.class})
    @Order(Integer.MIN_VALUE)
    public void init() {
        try {
            this.localHostName = InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            this.localHostName = InetAddress.getLoopbackAddress().getHostName();
        }
    }
}

