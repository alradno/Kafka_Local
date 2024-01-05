package com.example.kafka_local;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.example.kafka_local.Kafka_Elastic_Main.PROPS;
import java.io.IOException;

public class CustomProcessor implements Processor<String, String, String, List<List<String>>> {
    private ProcessorContext <String, List<List<String>>> context;
    private HistogramDisplay histogramDisplay;


    private List<Map<String, List<String>>> mapas;

    TypeReference<List<String>> typeRef;

    ObjectMapper mapper;

    public CustomProcessor() {
    }

    @Override
    public void init(ProcessorContext <String, List<List<String>>> context) {
        this.context = context;
        try {
            loadMaps();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        this.mapper = new ObjectMapper();
        this.typeRef = new TypeReference<>() {};
        histogramDisplay = new HistogramDisplay();
        histogramDisplay.setVisible(true);
    }

    @Override
    public void process(Record<String, String> record) {
        //long startTime = System.currentTimeMillis();

        String[] fields = record.value().split(";");

        //Lista para almacenar los campos procesados
        List<List<String>> newFields = new ArrayList<>();

        if (fields.length >= 6) {
            long startTime = System.nanoTime();
            for (int i = 0; i < 6; i++) {
                try {
                    List<String> list = this.mapas.get(i).get(fields[i]);
                    newFields.add(list);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            long endTime = System.nanoTime();
            double executionTime = endTime - startTime;
            histogramDisplay.addExecutionTime(executionTime / 1000);
            //crear una lista para cada campo restante
            for (int i = 6; i < fields.length; i++) {
                newFields.add(List.of(fields[i]));
            }
        }
        else{
            System.out.println("Error: No se pudo procesar el registro, falta alguno de los 6 campos iniciales");
        }

        context.forward(record.withValue(newFields));
        context.commit();

    }

    @Override
    public void close() {

        Processor.super.close();
    }

    public void loadMaps() throws IOException {

        this.mapas = List.of(new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>(), new HashMap<>());

        String bssmap_location = PROPS.getProperty("bssmap_file_location");
        String causainterna_location = PROPS.getProperty("causainterna_file_location");
        String ranap_location = PROPS.getProperty("ranap_file_location");
        String nrn_location = PROPS.getProperty("nrn_file_location");
        String operadores_location = PROPS.getProperty("operadores_file_location");
        String test_location = PROPS.getProperty("test_file_location");

        List<String> locations = List.of(bssmap_location, causainterna_location, ranap_location,
                nrn_location, operadores_location, test_location);

        int key_column = Integer.parseInt(PROPS.getProperty("key_column"));

        String line;
        int indice = 0;
        for (String location : locations){
            BufferedReader br = new BufferedReader(new FileReader(location));
            while ((line = br.readLine()) != null) {
                String[] columns = line.split(";");
                List<String> values = List.of(columns);
                String key = columns[key_column];
                mapas.get(indice).put(key, values);
            }
            indice++;
            br.close();
        }
        System.out.println("Mapas cargados en Local");
    }


}

