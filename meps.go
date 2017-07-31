/*
This script obtains national totals for 32 strata of the US
employer-sponsored insurance (ESI) population, based on the MEPS
survey data. The main MEPS site is here:

https://meps.ahrq.gov/mepsweb

Before running the script, first download the ASCII formatted versions
of the 'full year consolidated data files' for each year of interest
from this site:

https://meps.ahrq.gov/mepsweb/data_stats/download_data_files.jsp

Also download the SAS programming statements file for each year.

Next, compress the data files using gzip, and place everything into a
directory layout as follows:

|---2009
|    |---h129.dat.gz
|    |---h129su.txt
_
|---2010
|    |---h138.dat.gz
|    |---h138su.txt

Then run this script, after changing the dr variable below to point to
the location where the data are stored.

The strata are numbered 0 to 32 (so there are 33 strata in all).
Stratum 0 consists of people who are <18, >=65, or have missing data
on any of the four variables used to define the weight stratum.

Given a wgtkey value between 1 and 32 (inclusive), the characteristics
of its stratum can be recovered as follows:

female = (wgtkey - 1) % 2       // 1=female, 0=male
emprel = (wgtkey - 1) / 2 % 2   // 1=dependent, 0=policy holder
age    = (wgtkey - 1) / 4 % 2   // 0=born after 1967, 1=born on/before 1967
region = (wgtkey - 1) / 8       // 0=NE, 1=NC, 2=S, 3=W (census regions)
*/

package main

import (
	"bufio"
	"compress/gzip"
	"encoding/csv"
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	// Path to all MEPS data files
	dr string = "/nfs/kshedden/MEPS/data"
)

var (
	// Used to construct MEPS file names, each year has a 3-digit code number.
	yrnum = map[int]int{2009: 129, 2010: 138, 2011: 147, 2012: 155, 2013: 163,
		2014: 171, 2015: 174}

	// Name of overall weight variable for each year.
	wgtvar = map[int]string{2009: "PERWT09F", 2010: "PERWT10F", 2011: "PERWT11F",
		2012: "PERWT12F", 2013: "PERWT13F", 2014: "PERWT14F", 2015: "PERWT15P"}
)

// The data are in fixed-width format.  We can obtain the variable
// names and positions from the SAS format files.
func getvinf(year int) map[string][2]int {

	// SAS script file name and reader
	sp := fmt.Sprintf("h%dsu.txt", yrnum[year])
	sp = path.Join(dr, fmt.Sprintf("%d", year), sp)
	fid, err := os.Open(sp)
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	rdr := bufio.NewScanner(fid)

	// Map variable names to variable description.
	vdef := make(map[string][2]int)

	// Update the variable description for one variable.
	process := func(line string) {
		toks := strings.Fields(line)
		vname := toks[1]

		// Process the position
		pos := strings.TrimLeft(toks[0], "@")
		ipos, err := strconv.Atoi(pos)
		if err != nil {
			panic(err)
		}
		ipos -= 1 // want 0-based positions

		// Process the width
		w := strings.TrimLeft(toks[2], "$")
		if strings.Contains(w, ".") {
			toks := strings.Split(w, ".")
			w = toks[0]
		}
		iw, err := strconv.Atoi(w)
		if err != nil {
			panic(err)
		}

		vdef[vname] = [2]int{ipos, iw}
	}

	// Skip the initial section
	for rdr.Scan() {
		line := rdr.Text()
		if strings.Contains(line, "INPUT @1") {
			process(line[5:len(line)])
			break
		}
	}
	if err := rdr.Err(); err != nil {
		panic(err)
	}

	for rdr.Scan() {
		line := rdr.Text()

		if strings.HasPrefix(line, ";") {
			// Reached end of section of interest
			break
		}

		process(line[5:len(line)])
	}
	if err := rdr.Err(); err != nil {
		panic(err)
	}

	return vdef
}

func getpopw(year int) []float64 {

	vdef := getvinf(year)

	// 2 digit year, as string
	ys := fmt.Sprintf("%d", year)[2:]

	// The ESI population total by stratum
	sampsize := make([]float64, 33)

	dpath := fmt.Sprintf("h%d.dat.gz", yrnum[year])
	dpath = path.Join(dr, fmt.Sprintf("%d", year), dpath)
	hid, err := os.Open(dpath)
	if err != err {
		panic(err)
	}
	defer hid.Close()
	fid, err := gzip.NewReader(hid)
	if err != nil {
		panic(err)
	}
	scanner := bufio.NewScanner(fid)

	// Process one record
	ext := func(line string, vname string) float64 {
		u := vdef[vname]
		start := u[0]
		end := u[0] + u[1]
		x, err := strconv.ParseFloat(line[start:end], 64)
		if err != nil {
			panic(err)
		}
		return x
	}

	for scanner.Scan() {

		line := scanner.Text()

		insur := ext(line, "PEGJA"+ys)
		if insur != 1 {
			continue
		}

		dobyy := ext(line, "DOBYY")
		region := ext(line, "REGION"+ys)
		wgt := ext(line, wgtvar[year])
		emprel := ext(line, "HPEJA"+ys)

		female := ext(line, "SEX")
		if female != 1 && female != 2 {
			continue
		}
		female -= 1

		// age stratum
		var age float64
		switch {
		case dobyy < 2009-65:
			continue // too old
		case dobyy < 2012-45:
			age = 1
		case dobyy < 2012-18:
			age = 0
		default:
			continue // too young
		}

		// region
		if region < 0 {
			continue
		}
		region -= 1

		// emprel
		if emprel == -1 {
			continue
		}
		emprel -= 1

		stratum := int(1 + female + 2*emprel + 4*age + 8*region)

		sampsize[stratum] += wgt
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	return sampsize
}

func main() {
	dw := make(map[int][]float64)
	for _, y := range []int{2009, 2010, 2011, 2012, 2013, 2014, 2015} {
		ss := getpopw(y)
		dw[y] = ss
	}

	fid, err := os.Create("meps_totals.csv")
	if err != nil {
		panic(err)
	}
	defer fid.Close()
	wtr := csv.NewWriter(fid)

	recs := make([]string, 7)

	for i := 0; i < 7; i++ {
		recs[i] = fmt.Sprintf("%d", 2009+i)
	}
	wtr.Write(recs)

	for i := 0; i < 33; i++ {
		for j := 0; j < 7; j++ {
			recs[j] = fmt.Sprintf("%.0f", dw[2009+j][i])
		}
		err := wtr.Write(recs)
		if err != nil {
			panic(err)
		}
	}

	wtr.Flush()
}
