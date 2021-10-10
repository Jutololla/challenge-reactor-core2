package com.example.demo;

import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvException;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class CSVUtilTest {

    @Test
    void converterData(){
        List<Player> list = CsvUtilFile.getPlayers();
        assert list.size() == 18207;
    }

    @Test
    void stream_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.club.equals(playerB.club))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getClub));

        assert listFilter.size() == 322;
    }


    @Test
    void reactive_filtrarJugadoresMayoresA35(){
        List<Player> list = CsvUtilFile.getPlayers();
        Flux<Player> listFlux = Flux.fromStream(list.parallelStream()).cache();
        Mono<Map<String, Collection<Player>>> listFilter = listFlux
                .filter(player -> player.age >= 35)
                .map(player -> {
                    player.name = player.name.toUpperCase(Locale.ROOT);
                    return player;
                })
                .buffer(100)
                .flatMap(playerA -> listFlux
                         .filter(playerB -> playerA.stream()
                                 .anyMatch(a ->  a.club.equals(playerB.club)))
                )
                .distinct()
                .collectMultimap(Player::getClub);

        assert listFilter.block().size() == 322;
    }


    @Test
    @DisplayName("Filtra los jugadores mayores a 34 años  de un club especifico : OGCNice")
    void stream_filtrarJugadoresMayoresA34PorClubEspecifico(){
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .filter(player -> player.club.equalsIgnoreCase("OGC Nice"))
                .filter(player1 -> player1.age >= 34)
                .collect(Collectors.groupingBy(Player::getName));

        listFilter.forEach((s,player)-> player.stream().forEach(_player ->
                System.out.println("Nombre: " + _player.name + " " + "Edad: " + _player.age)));

        assert listFilter.size() == 2;
    }

    @Test
    @DisplayName("Filtra los jugadores por pais, y luego rankea los jugadores de cada pais en base a sus partidos ganados")
    void stream_filtrarJugadoresPorPaisRanking(){
        List<Player> list = CsvUtilFile.getPlayers();
        Map<String, List<Player>> listFilter = list.parallelStream()
                .flatMap(playerA -> list.parallelStream()
                        .filter(playerB -> playerA.national.equals(playerB.national))
                        .sorted((a,b) -> Math.max(a.winners,b.winners))
                )
                .distinct()
                .collect(Collectors.groupingBy(Player::getNational));

        listFilter.forEach((s,player)-> player.stream().forEach(_player ->
                System.out.println("Nombre: " + _player.name + "   Nacionalidad: " + _player.national
                        + "   Partidos Ganados: " + _player.winners)));

        System.out.println(listFilter.size());
    }


}
