package ru.curs.counting.configuration;

import lombok.RequiredArgsConstructor;
import org.fluttercode.datafactory.impl.DataFactory;
import org.springframework.stereotype.Service;

import java.util.Arrays;
import java.util.function.Supplier;

@Service
@RequiredArgsConstructor
public class BettorSupplier implements Supplier<String> {

    private final DataFactory df;

    @Override
    public String get() {
        return df.getItem(Arrays.asList(
                "Sabrina Reid",
                "Autumn Wade",
                "Lewis Bryant",
                "David Cooley",
                "Julia Daniel",
                "Roy Britt",
                "Allison Farrell",
                "Lance Ryan",
                "Loretta Dunlap",
                "Unborn Lawson",
                "Candy Cochran",
                "Norma Clayton",
                "Joyce Sharp",
                "Tabatha Yates",
                "Robin Bass",
                "Rhonda Jennings",
                "Dorothy Griffin",
                "Rose Camacho",
                "Martha Herman",
                "Christoph Raymond"));
    }
}
