-- MySQL Script generated by MySQL Workbench
-- Fri Jan  4 13:59:08 2019
-- Model: New Model    Version: 1.0
-- MySQL Workbench Forward Engineering

SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0;
SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0;
SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='TRADITIONAL,ALLOW_INVALID_DATES';

-- -----------------------------------------------------
-- Schema retraction
-- -----------------------------------------------------

-- -----------------------------------------------------
-- Schema retraction
-- -----------------------------------------------------
CREATE SCHEMA IF NOT EXISTS `retraction` DEFAULT CHARACTER SET utf8mb4 ;
USE `retraction` ;

-- -----------------------------------------------------
-- Table `retraction`.`author`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`author` (
  `idauthor` INT(11) NOT NULL AUTO_INCREMENT,
  `given_names` VARCHAR(500) NULL DEFAULT NULL,
  `last_name` VARCHAR(500) NULL DEFAULT NULL,
  `h-index` INT(11) NULL DEFAULT NULL,
  `orcid` VARCHAR(500) NULL DEFAULT NULL,
  `corporate` INT(1) NULL DEFAULT '0',
  PRIMARY KEY (`idauthor`),
  UNIQUE INDEX `orcid_UNIQUE` (`orcid` ASC))
ENGINE = InnoDB
AUTO_INCREMENT = 2780411
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`discipline`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`discipline` (
  `iddiscipline` INT(11) NOT NULL AUTO_INCREMENT,
  `name` VARCHAR(45) NULL DEFAULT NULL,
  `idsubfield` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`iddiscipline`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`journal`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`journal` (
  `idjournal` INT(11) NOT NULL AUTO_INCREMENT,
  `title` VARCHAR(1000) NULL DEFAULT NULL,
  `issn` VARCHAR(9) NULL DEFAULT NULL,
  `essn` VARCHAR(9) NULL DEFAULT NULL,
  `short_title` VARCHAR(1000) NULL DEFAULT NULL,
  PRIMARY KEY (`idjournal`))
ENGINE = InnoDB
AUTO_INCREMENT = 172
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`paper`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`paper` (
  `doi` VARCHAR(150) NULL DEFAULT NULL,
  `title` VARCHAR(1000) NULL DEFAULT NULL,
  `publication_date` DATE NULL DEFAULT NULL,
  `abstract` TEXT NULL DEFAULT NULL,
  `open_access` TINYINT(1) NULL DEFAULT NULL,
  `url` VARCHAR(2083) NULL DEFAULT NULL,
  `idjournal` INT(11) NULL DEFAULT NULL,
  `iddiscipline` INT(11) NULL DEFAULT NULL,
  `idpaper` INT(11) NOT NULL AUTO_INCREMENT,
  `first_page` VARCHAR(10) NULL DEFAULT NULL,
  `last_page` VARCHAR(10) NULL DEFAULT NULL,
  `time_added` DATETIME NULL DEFAULT NULL,
  `content` LONGTEXT NULL DEFAULT NULL,
  `cited_records` LONGTEXT NULL DEFAULT NULL,
  `wos_identifier` VARCHAR(150) NULL DEFAULT NULL,
  `total_citations` INT NULL DEFAULT NULL,
  `citation_record` LONGTEXT NULL DEFAULT NULL,
  `retracted_date` DATE NULL DEFAULT NULL,
  PRIMARY KEY (`idpaper`),
  UNIQUE INDEX `idx_paper_doi` (`doi` ASC),
  UNIQUE INDEX `wos_identifier_UNIQUE` (`wos_identifier` ASC),
  INDEX `idjournal_idx` (`idjournal` ASC),
  INDEX `iddiscipline_idx` (`iddiscipline` ASC),
  CONSTRAINT `fk_paper_discipline`
    FOREIGN KEY (`iddiscipline`)
    REFERENCES `retraction`.`discipline` (`iddiscipline`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_paper_journal`
    FOREIGN KEY (`idjournal`)
    REFERENCES `retraction`.`journal` (`idjournal`)
    ON DELETE NO ACTION
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 697983
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`citation`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`citation` (
  `source_id` INT(11) NOT NULL,
  `target_id` INT(11) NOT NULL,
  PRIMARY KEY (`source_id`, `target_id`),
  INDEX `fk_citation_paper2_idx` (`target_id` ASC),
  CONSTRAINT `fk_citation_paper1`
    FOREIGN KEY (`source_id`)
    REFERENCES `retraction`.`paper` (`idpaper`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_citation_paper2`
    FOREIGN KEY (`target_id`)
    REFERENCES `retraction`.`paper` (`idpaper`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`modularity_measure`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`modularity_measure` (
  `measure` VARCHAR(45) NOT NULL,
  `label` VARCHAR(45) NULL DEFAULT NULL,
  `description` VARCHAR(1024) NULL DEFAULT NULL,
  PRIMARY KEY (`measure`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`modularity_class`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`modularity_class` (
  `idmodularity_class` INT(11) NOT NULL AUTO_INCREMENT,
  `measure` VARCHAR(45) NOT NULL,
  `classification` INT(11) NOT NULL,
  `label` VARCHAR(45) NULL DEFAULT NULL,
  PRIMARY KEY (`idmodularity_class`),
  UNIQUE INDEX `uq_measure_classification` (`measure` ASC, `classification` ASC),
  INDEX `fk_modularity_class_modularity_measure_idx` (`measure` ASC),
  CONSTRAINT `fk_modularity_class_modularity_measure`
    FOREIGN KEY (`measure`)
    REFERENCES `retraction`.`modularity_measure` (`measure`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 13806
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`network`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`network` (
  `network_key` VARCHAR(45) NOT NULL,
  `label` VARCHAR(45) NULL DEFAULT NULL,
  `description` VARCHAR(1024) NULL DEFAULT NULL,
  `ref_column` VARCHAR(45) NOT NULL DEFAULT 'idpaper',
  `directed` TINYINT(1) NOT NULL DEFAULT '0',
  PRIMARY KEY (`network_key`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`network_edges`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`network_edges` (
  `idnetwork_edges` INT(11) NOT NULL AUTO_INCREMENT,
  `network_key` VARCHAR(45) NULL DEFAULT NULL,
  `source` INT(11) NULL DEFAULT NULL,
  `target` INT(11) NULL DEFAULT NULL,
  `weight` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`idnetwork_edges`),
  UNIQUE INDEX `uq_network_source_target` (`network_key` ASC, `source` ASC, `target` ASC),
  INDEX `fk_network_edges_network_idx` (`network_key` ASC),
  INDEX `fk_network_edges_source_paper_idx` (`source` ASC),
  INDEX `fk_network_edges_target_paper_idx` (`target` ASC),
  CONSTRAINT `fk_network_edges_network`
    FOREIGN KEY (`network_key`)
    REFERENCES `retraction`.`network` (`network_key`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_network_edges_source_paper`
    FOREIGN KEY (`source`)
    REFERENCES `retraction`.`paper` (`idpaper`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION,
  CONSTRAINT `fk_network_edges_target_paper`
    FOREIGN KEY (`target`)
    REFERENCES `retraction`.`paper` (`idpaper`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB
AUTO_INCREMENT = 16617021
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`paper_author`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`paper_author` (
  `idauthor` INT(11) NOT NULL,
  `idpaper` INT(11) NOT NULL,
  PRIMARY KEY (`idauthor`, `idpaper`),
  INDEX `idauthor_idx` (`idauthor` ASC),
  INDEX `idpaper_idx` (`idpaper` ASC),
  CONSTRAINT `fk_paper_author_author`
    FOREIGN KEY (`idauthor`)
    REFERENCES `retraction`.`author` (`idauthor`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_paper_author_paper`
    FOREIGN KEY (`idpaper`)
    REFERENCES `retraction`.`paper` (`idpaper`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`paper_keyword`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`paper_keyword` (
  `idpaper_keyword` INT(11) NOT NULL AUTO_INCREMENT,
  `keyword` VARCHAR(100) NULL DEFAULT NULL,
  `idpaper` INT(11) NULL DEFAULT NULL,
  PRIMARY KEY (`idpaper_keyword`),
  INDEX `idpaper_idx` (`idpaper` ASC),
  CONSTRAINT `fk_paper_keyword_paper`
    FOREIGN KEY (`idpaper`)
    REFERENCES `retraction`.`paper` (`idpaper`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
AUTO_INCREMENT = 3942774
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`paper_modularity_class`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`paper_modularity_class` (
  `idpaper` INT(11) NOT NULL,
  `idmodularity_class` INT(11) NOT NULL,
  PRIMARY KEY (`idpaper`, `idmodularity_class`),
  INDEX `fk_paper_modularity_class_paper_idx` (`idpaper` ASC),
  INDEX `fk_paper_modularity_class_modularity_class_idx` (`idmodularity_class` ASC),
  CONSTRAINT `fk_paper_modularity_class_modularity_class`
    FOREIGN KEY (`idmodularity_class`)
    REFERENCES `retraction`.`modularity_class` (`idmodularity_class`)
    ON DELETE CASCADE
    ON UPDATE CASCADE,
  CONSTRAINT `fk_paper_modularity_class_paper`
    FOREIGN KEY (`idpaper`)
    REFERENCES `retraction`.`paper` (`idpaper`)
    ON DELETE CASCADE
    ON UPDATE CASCADE)
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`title_short_title`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`title_short_title` (
  `title` VARCHAR(1000) NULL DEFAULT NULL,
  `short_title` VARCHAR(1000) NULL DEFAULT NULL,
  `idtitle_short_title` INT(11) NOT NULL AUTO_INCREMENT,
  PRIMARY KEY (`idtitle_short_title`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8mb4;


-- -----------------------------------------------------
-- Table `retraction`.`citation_history`
-- -----------------------------------------------------
CREATE TABLE IF NOT EXISTS `retraction`.`citation_history` (
  `idcitation_history` INT NOT NULL,
  `idpaper` INT(11) NULL,
  `start_year` YEAR NULL,
  `yearly_citations` TEXT NULL,
  `year_count` INT NULL,
  PRIMARY KEY (`idcitation_history`),
  INDEX `fk_citation_history_paper_idx` (`idpaper` ASC),
  CONSTRAINT `fk_citation_history_paper`
    FOREIGN KEY (`idpaper`)
    REFERENCES `retraction`.`paper` (`idpaper`)
    ON DELETE NO ACTION
    ON UPDATE NO ACTION)
ENGINE = InnoDB;


SET SQL_MODE=@OLD_SQL_MODE;
SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS;
SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS;
