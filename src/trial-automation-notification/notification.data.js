module.exports = {
    trialActivate(courseName, duration) {
        return {
            title: `🔴${courseName} ${duration} Day trial Activated`,
            subtitle: "Start karo Apni taiyari",
            firebaseTag: "trial_active",
        };
    },
    trialStart: {
        firstHour(courseName) {
            return {
                title: "🔴 Free trial ka faida uthao",
                subtitle: `${courseName} ki taraf badhao apna pehla kadam.`,
                firebaseTag: "trial_start",
            };
        },
        secondHour(courseName) {
            return {
                title: `🔴 ${courseName} Free trial ko yun hi mat jane do`,
                subtitle: "Start karo apni padhai",
                firebaseTag: "trial_start",
            };
        },
    },
    trialReturn6(courseName) {
        return {
            title: `🔴${courseName} ki tayari rakho jari`,
            subtitle: "Free trial Active!",
            firebaseTag: "trial_return_6",
        };
    },
    trialReturn12(count) {
        return {
            title: "🔴Keep Learning!",
            subtitle: `${count} Students kar rahe apni IBPS ki taiyari pakki`,
            firebaseTag: "trial_return_12",
        };
    },
    trialContinue: {
        trigger1: {
            title: "🔴 Ruko Mat!",
            subtitle: "Jari Rakho apni taiyari",
            firebaseTag: "trial_continue",
        },
        trigger2(facultyName) {
            return {
                title: `🔴${facultyName} se padh k accha laga?`,
                subtitle: "Padhna Jari rakho.",
                firebaseTag: "trial_continue",
            };
        },
    },
    trialMorning(courseName) {
        return {
            title: `🔴 To shuru karein ${courseName} ki aaj ki taiyari?`,
            subtitle: "Jaldi Join karo",
            firebaseTag: "trial_morning",
        };
    },
    trialNight(courseName) {
        return {
            title: "🔴 Hi, Night Owl🦉.",
            subtitle: `${courseName} ki padhai sapna nahi hakeekat banao. Keep Studying`,
            firebaseTag: "trial_night",
        };
    },
    trialEndDay1(courseName) {
        return {
            title: "🔴 Ruko Mat",
            subtitle: `${courseName} ki apni taiyari rakho jaari`,
            firebaseTag: "trial_end_day1",
        };
    },
    trialEndDay2(courseName) {
        return {
            title: "🔴 Sochna choro padhna shuru karo!",
            subtitle: `${courseName} mein Karo apni seat pakki`,
            firebaseTag: "trial_end_day2",
        };
    },
};
